require 'mysql2'
require 'cassandra'
require 'benchmark'

require_relative './tables'
require_relative './plans'
require_relative './cassandra'

record_num = 400

table_names = [[:users_id, :users_rating_secondary, :items_id, :items_quantity],
               [:users_id, :users_rating_secondary, :items_id,:items_quantity],
               [:users_id, :users_rating, :items_id,:items_quantity_secondary]]

data = {}
my_client = Mysql2::Client.new(host: "127.0.0.1", username: "root", password: "root", database: 'rubis')
data[:users] = my_client.query("SELECT * FROM users LIMIT #{record_num}");
data[:items] = my_client.query("SELECT * FROM items LIMIT #{record_num}");
#data[:users] = my_client.query("SELECT * FROM users");
#data[:items] = my_client.query("SELECT * FROM items");

cassandraManager = CassandraManager.new('rubis')

tables = Tables.new.tables
tables.each do |_, table|
  cassandraManager.dropTable(table)
end

users_id = 'SELECT users.* FROM users WHERE users.id = ? -- 8'
users_rating = 'SELECT users.* FROM users WHERE users.rating = ? -- 8'
items_id = 'SELECT items.* FROM items WHERE items.id=? -- 13'
items_quantity = 'SELECT items.* FROM items WHERE items.quantity=? -- 13'

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

table_names.each_with_index do |t_names, timestep|
  cfs = t_names.map{|n| tables[n]}
  cfs.each do |cf|
    cassandraManager.dropTable(cf)
    cassandraManager.createTable(cf)
    cassandraManager.insert_data(data, cf)
  end

  users_initial = cassandraManager.executeQuery("SELECT rating, id FROM #{tables[:users_id].name}")
  items_initial = cassandraManager.executeQuery("SELECT quantity, id FROM #{tables[:items_id].name}")

  Benchmark.bm do |x|
    x.report("users_id"){ users_id_result = tdp_users_id.plans[timestep].execute(users_initial, cassandraManager)}
    x.report("users_rating"){ users_rating_result = tdp_users_rating.plans[timestep].execute(users_initial, cassandraManager)}
    x.report("items_id"){ items_id_result = tdp_items_id.plans[timestep].execute(items_initial, cassandraManager)}
    x.report("items_quantity"){ items_quantity_result = tdp_items_quantity.plans[timestep].execute(items_initial, cassandraManager)}
  end
  break;
end
