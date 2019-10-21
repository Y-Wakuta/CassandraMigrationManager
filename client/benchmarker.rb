require 'mysql2'
require 'cassandra'
require 'benchmark'

require_relative './tables'
require_relative './plans'
require_relative './cassandra'
require_relative './migrate'
require_relative './worker'

def output_whole_average(result, timestep)
  puts "=#{timestep}================"
  puts result.map{|r| r[:exec_time]}.flatten(1).sum / result.map{|r| r[:exec_time]}.flatten(1).length
end

def output_average(result, timestep)
  result = result.map{|r| {query: r[:query], exec_time: r[:exec_time].sum / r[:exec_time].length}}

  puts "=#{timestep}================"
  result.each do |r|
    puts "query: #{r[:query]}, exec_time: #{r[:exec_time]}"
  end
end

def exec_benchmark(td_plans, data, interval, repeat)
  cassandraManager = CassandraManager.new('rubis')

  cassandraManager.initKeyspace(td_plans.map{|tdp| tdp.plans[0].steps}.flatten(1).to_set, data)

  start = Time.now
  (0...td_plans.first.plans.size).each do |timestep|
    initial = td_plans.map{|tdp| tdp.plans[timestep].query_initial_condition(repeat)}.reduce(Hash.new){|b, n| b.merge!(n)}

    workers = td_plans.map{|tdp| Worker.new {|_| tdp.plans[timestep].execute_bench(initial)} }
    workers.map(&:run).each(&:join)

    threads = workers.map{|worker|
      worker.execute
    }
    threads.each(&:join)

    migration_plans_this_timestep = td_plans.map{|tdp| tdp.find_migration}
                                            .flatten
                                            .select{|mp| mp.start_timestep == timestep}
    migration_plans_this_timestep.each do |mp|
      Migrate.exec_migration(mp, cassandraManager, initial)
    end

    now = Time.now
    _next = [start + interval, now].max
    sleep(_next - now)
    start = _next

    workers.each(&:stop)

    plans_for_timestep = td_plans.map{|tdp| tdp.plans[timestep]}
    migration_plans_this_timestep.each do |mp|
      Migrate.drop_obsolete_tables(mp, cassandraManager, plans_for_timestep)
    end

    result = workers.map(&:read_from_child)
    output_average(result, timestep)
    output_whole_average(result, timestep)
  end
end

record_num = 5000
my_client = Mysql2::Client.new(host: "127.0.0.1", username: "root", password: "root", database: 'rubis')
#my_client = Mysql2::Client.new(host: "mysql", username: "root", password: "root", database: 'rubis')

data = {}
data[:users] = my_client.query("SELECT * FROM users LIMIT #{record_num}");
data[:items] = my_client.query("SELECT * FROM items LIMIT #{record_num}");

tables = Tables.new.tables
q_users_id = {query: 'SELECT users.* FROM users WHERE users.id = ? -- 8', frequency: [0.1, 50, 900]}
q_users_rating = {query: 'SELECT users.* FROM users WHERE users.rating = ? -- 8', frequency:[0.1, 50, 900]}
q_items_id = {query: 'SELECT items.* FROM items WHERE items.id=? -- 13', frequency: [900, 50, 0.1]}
q_items_quantity = {query: 'SELECT items.* FROM items WHERE items.quantity=? -- 13', frequency: [900, 50, 0.1]}

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

p_users_rating_static = [
    [:users_rating_secondary, :users_id],
    [:users_rating_secondary, :users_id],
    [:users_rating_secondary, :users_id],
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

p_items_quantity_static = [
    [:items_quantity],
    [:items_quantity],
    [:items_quantity],
].map{|tns| tns.map{|tn| tables[tn]}}

interval = 30
td_plans_time_depend = []
td_plans_time_depend << TimeDependPlan.new(q_users_id, p_users_id, interval)
td_plans_time_depend << TimeDependPlan.new(q_items_id, p_items_id, interval)
td_plans_time_depend << TimeDependPlan.new(q_users_rating,p_users_rating, interval)
td_plans_time_depend << TimeDependPlan.new(q_items_quantity,p_items_quantity, interval)

td_plans_static = []
td_plans_static << TimeDependPlan.new(q_users_id, p_users_id, interval)
td_plans_static << TimeDependPlan.new(q_items_id, p_items_id, interval)
td_plans_static << TimeDependPlan.new(q_users_rating,p_users_rating_static, interval)
td_plans_static << TimeDependPlan.new(q_items_quantity,p_items_quantity_static, interval)

puts "\e[36m time depend \e[0m"
repeat = 1
exec_benchmark(td_plans_time_depend, data, interval, repeat)

puts "\e[36m static \e[0m"
exec_benchmark(td_plans_static, data, interval, repeat)
