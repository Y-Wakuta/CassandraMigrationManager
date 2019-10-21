require 'mysql2'
require 'cassandra'
require 'benchmark'

require_relative './tables'
require_relative './plans'
require_relative './cassandra'
require_relative './migrate'
require_relative './worker'

record_num = 100
interval = 5

my_client = Mysql2::Client.new(host: "127.0.0.1", username: "root", password: "root", database: 'rubis')

data = {}
data[:users] = my_client.query("SELECT * FROM users LIMIT #{record_num}");
data[:items] = my_client.query("SELECT * FROM items LIMIT #{record_num}");

cassandraManager = CassandraManager.new('rubis')

tables = Tables.new.tables
q_users_id = {query: 'SELECT users.* FROM users WHERE users.id = ? -- 8', frequency: [10000,20000,30000]}
q_users_rating = {query: 'SELECT users.* FROM users WHERE users.rating = ? -- 8', frequency: [10000,20000,30000]}
q_items_id = {query: 'SELECT items.* FROM items WHERE items.id=? -- 13', frequency: [30000,20000,10000]}
q_items_quantity = {query: 'SELECT items.* FROM items WHERE items.quantity=? -- 13', frequency: [30000,20000,10000]}

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

td_plans = []
td_plans << TimeDependPlan.new(q_users_id, p_users_id )
td_plans << TimeDependPlan.new(q_users_rating,p_users_rating)
td_plans << TimeDependPlan.new(q_items_id, p_items_id)
td_plans << TimeDependPlan.new(q_items_quantity,p_items_quantity)

migration_plans = td_plans.map{|tdp| tdp.find_migration}.flatten!

cassandraManager.clearKeyspace
td_plans.map{|tdp| tdp.plans[0].steps}.flatten(1).to_set.each do |cf|
  cassandraManager.createTable(cf)
  cassandraManager.insert_data(data, cf)
end

(0...td_plans.first.plans.size).each do |timestep|
  initial = {}
  td_plans.map{|tdp| tdp.plans[timestep].steps.first}.flatten(1).to_set.each do |table|
    initial[table] = cassandraManager.executeQuery("SELECT * FROM #{table.name}")
  end

  workers = td_plans.map{|tdp| Worker.new {|_| tdp.plans[timestep].execute_bench(initial)} }
  workers.map(&:run).each(&:join)

  threads = workers.map{|worker|
    worker.execute
  }
  threads.each(&:join)

  sleep(interval)

  migration_plans.flatten(1).select{|mp| mp.start_timestep == timestep}.each do |mp|
    Migrate.exec_migration(mp, cassandraManager, initial)
  end
  workers.each(&:stop)

  plans_for_timestep = td_plans.map{|tdp| tdp.plans[timestep]}
  migration_plans.flatten(1).select{|mp| mp.start_timestep == timestep}.each do |mp|
    Migrate.drop_obsolete_tables(mp, cassandraManager, plans_for_timestep)
  end

  result = workers.map(&:read_from_child)
  result = result.map{|r| {query: r[:query], exec_time: r[:exec_time].sum / r[:exec_time].length}}
  puts "=#{timestep}================"
  result.each do |r|
    puts "query: #{r[:query]}, exec_time: #{r[:exec_time]}"
  end
end

