require 'mysql2'
require 'cassandra'

require_relative './tables'
require_relative './plans'


def get_data_from_mysql
  record_num = 50
  my_client = Mysql2::Client.new(host: "127.0.0.1", username: "root", password: "root", database: 'rubis')
  users = my_client.query("SELECT * FROM users LIMIT #{record_num}");
  items = my_client.query("SELECT * FROM items LIMIT #{record_num}");
  [users, items]
end

def setup_cassandra(tables)
  cluster = Cassandra.cluster
  cluster.each_host do |host|
    puts "Host #{host.ip}: id=#{host.id} datacenter=#{host.datacenter} rack=#{host.rack}"
  end
  session = cluster.connect('rubis')
  session.execute('USE rubis')
  tables.each do |table|
    session.execute("DROP TABLE IF EXISTS #{table.name}")
  end
  session
end

# creating initial schema does not included to benchmark
def setup_initial_schema(session, datas, tables, table_names)
  table_names.first.each do |name|
    t = tables.find{|t| t.name == name}
    session.execute(t.create)

    insert_data(session, datas, tables, name)
  end
end

def insert_data(session, datas, tables, name)
  t = tables.find{|t| t.name == name}
  prepared = session.prepare t.insert
  batch = session.batch do |b|
    datas[t.entity].to_a.each do |data|
      b.add(prepared, arguments:
          t.all_fields.sort_by { |f | f.name}.map do |field|
            field.name == :id ? Cassandra::Uuid.new(data[field.name.to_s])
                : data[field.name.to_s]
          end.to_a
      )
    end
  end
  session.execute(batch)
end

def prepare_queries(session)
  prepareds = {}
  prepareds[:q_user_id] = session.prepare
  prepareds[:q_user_rating] = session.prepare
  prepareds[:q_items_id] =  session.prepare
  prepareds[:q_items_quantity] = session.prepare
  prepareds
end

def setup_steps(tables, session)
  steps = {}
  tables.map do |table|
    prepared = session.prepare table.select
    steps[query_name] = Proc.new do |params|
      batch = session.batch do |b|
        params.to_a.each do |param|
          b.add prepared, arguments: [param]
        end
      end
      session.execute(batch)
    end
  end
end

table_names = [["users_id", "users_rating_secondary", "items_id", "items_quantity"],
               [ "users_id", "users_rating_secondary", "items_id","items_quantity"],
               [ "users_id", "users_rating", "items_id", "items_quantity_secondary"]]

users ,items = get_data_from_mysql
tables = Tables.new.tables
session = setup_cassandra tables

datas = {
    users: users,
    items: items
}
setup_initial_schema(session, datas, tables, table_names)

prepared_queries = prepare_queries(session)
steps = setup_steps(tables, session)

#user_id_materialized = Plan.new()

p_user_id = TimeDependPlan.new(plans,'SELECT users.* FROM users WHERE users.id = ? -- 8')
p_user_rating = TimeDependPlan.new(plans,'SELECT users.* FROM users WHERE users.rating = ? -- 8')
p_items_id = TimeDependPlan.new(plans,'SELECT items.* FROM items WHERE items.id=? -- 13')
p_items_quantity = TimeDependPlan.new(plans,'SELECT items.* FROM items WHERE items.quantity=? -- 13')

plans.plans << p
