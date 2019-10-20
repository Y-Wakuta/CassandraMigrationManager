require 'mysql2'
require 'cassandra'
require 'benchmark'

require_relative './tables'
require_relative './plans'
require_relative './cassandra'
require_relative './migrate'
require_relative './worker'

record_num = 100
interval = 10

table_names = [[:users_id, :users_rating_secondary, :items_id, :items_quantity],
               [:users_id, :users_rating_secondary, :items_id,:items_quantity],
               [:users_id, :users_rating, :items_id,:items_quantity_secondary]]

data = {}
my_client = Mysql2::Client.new(host: "127.0.0.1", username: "root", password: "root", database: 'rubis')
data[:users] = my_client.query("SELECT * FROM users LIMIT #{record_num}");
data[:items] = my_client.query("SELECT * FROM items LIMIT #{record_num}");

cassandraManager = CassandraManager.new('rubis')

tables = Tables.new.tables
users_id = {query: 'SELECT users.* FROM users WHERE users.id = ? -- 8', frequency: [1,2,3]}
users_rating = {query: 'SELECT users.* FROM users WHERE users.rating = ? -- 8', frequency: [1,2,3]}
items_id = {query: 'SELECT items.* FROM items WHERE items.id=? -- 13', frequency: [1,2,3]}
items_quantity = {query: 'SELECT items.* FROM items WHERE items.quantity=? -- 13', frequency: [1,2,3]}

p_users_id = [
    [:users_id],
    [:users_id],
    [:users_id],
].map{|tns| tns.map{|tn| tables[tn]}}

p_users_rating = [
    [:users_rating_secondary, :users_id],
    [:users_rating_secondary, :users_id],
    [:users_rating],
].map{|tns| tns.map{|tn| tables[tn]}}

p_items_id = [
    [:items_id],
    [:items_id],
    [:items_id],
].map{|tns| tns.map{|tn| tables[tn]}}

p_items_quantity = [
    [:items_quantity],
    [:items_quantity],
    [:items_quantity_secondary, :items_id],
].map{|tns| tns.map{|tn| tables[tn]}}

tdp_users_id = TimeDependPlan.new(p_users_id,users_id)
tdp_users_rating = TimeDependPlan.new(p_users_rating,users_rating)
tdp_items_id = TimeDependPlan.new(p_items_id, items_id)
tdp_items_quantity = TimeDependPlan.new(p_items_quantity, items_quantity)

migration_plans = []
migration_plans << Migrate.find_migration(tdp_users_id.plans)
migration_plans << Migrate.find_migration(tdp_users_rating.plans)
migration_plans << Migrate.find_migration(tdp_items_id.plans)
migration_plans << Migrate.find_migration(tdp_items_quantity.plans)
migration_plans.flatten!

initial = {}

users_id_result = []
users_rating_result = []
items_id_result = []
items_quantity_result = []

cassandraManager.clearKeyspace
table_names.each_with_index do |t_names, timestep|
  cfs = t_names.map{|n| tables[n]}
  cfs.each do |cf|
    cassandraManager.dropTable(cf)
    cassandraManager.createTable(cf)
    cassandraManager.insert_data(data, cf)
  end

  initial[:users] = cassandraManager.executeQuery("SELECT rating, id FROM #{tables[:users_id].name}")
  initial[:items] = cassandraManager.executeQuery("SELECT quantity, id FROM #{tables[:items_id].name}")

  workers = []
  workers << Worker.new {|_| tdp_users_id.plans[timestep].execute_bench(initial[:users])}
  workers << Worker.new {|_| tdp_users_rating.plans[timestep].execute_bench(initial[:users] )}
  workers << Worker.new {|_| tdp_items_id.plans[timestep].execute_bench(initial[:items])}
  workers << Worker.new {|_| tdp_items_quantity.plans[timestep].execute_bench(initial[:items])}

  workers.map(&:run).each(&:join)

  threads = workers.map{|worker|
    worker.execute
  }
  threads.each(&:join)

  # x.report("users_id"){ users_id_result << tdp_users_id.plans[timestep].execute(initial[:users], cassandraManager)}
  # x.report("users_rating"){ users_rating_result << tdp_users_rating.plans[timestep].execute(initial[:users], cassandraManager)}
  # x.report("items_id"){ items_id_result << tdp_items_id.plans[timestep].execute(initial[:items], cassandraManager)}
  # x.report("items_quantity"){ items_quantity_result << tdp_items_quantity.plans[timestep].execute(initial[:items], cassandraManager)}

  sleep(interval * 2)
  workers.each(&:stop)

  hoge = workers.map(&:read_from_child)

  migration_plans.flatten(1).select{|mp| mp.start_timestep == timestep}.each do |mp|
    plans_for_timestep = [tdp_users_id, tdp_users_rating, tdp_items_id, tdp_items_quantity].map{|tdp| tdp.plans[timestep]}
    Migrate.exec_migration(mp, cassandraManager, initial, plans_for_timestep)
  end
end

if [users_id_result, users_rating_result, items_id_result, items_quantity_result].any?{|res| res.uniq.size > 1}
  fail "the result of the query changes for each timestep"
end

