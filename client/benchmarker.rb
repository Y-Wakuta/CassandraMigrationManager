require 'mysql2'
require 'cassandra'

require_relative './tables'

def get_data_from_mysql
  my_client = Mysql2::Client.new(host: "127.0.0.1", username: "root", password: "root", database: 'rubis')
  users = my_client.query("SELECT * FROM users LIMIT 50");
  items = my_client.query("SELECT * FROM items LIMIT 50");
  return users, items
end

def setup_cassandra(tables)
  cluster = Cassandra.cluster

  cluster.each_host do |host|
    puts "Host #{host.ip}: id=#{host.id} datacenter=#{host.datacenter} rack=#{host.rack}"
  end
  session = cluster.connect('rubis')
  session.execute('USE rubis')
  session.execute("DROP TABLE IF EXISTS users_id")
  session.execute("DROP TABLE IF EXISTS users_rating")
  session.execute("DROP TABLE IF EXISTS users_rating_secondary")
  session.execute("DROP TABLE IF EXISTS items_id")
  session.execute("DROP TABLE IF EXISTS items_quantity")
  session.execute("DROP TABLE IF EXISTS items_quantity_secondary")
  session
end

# creating initial schema does not included to benchmark
def setup_initial_schema(session, datas, tables)
  session.execute(tables.cf_users_id)
  session.execute(tables.cf_users_rating_secondary)
  session.execute(tables.cf_items_quantity)
end

def load_data(cf_name, rows)
  prepared = "INSERT INTO \"#{cf_name}\" (" \
                   "#{field_names fields}" \
                   ") VALUES (#{(['?'] * fields.length).join ', '})"
  prepared = client.prepare prepared

end

users, items = get_data_from_mysql()
tables = Tables.new
session = setup_cassandra tables

datas = {
    users: users,
    items: items
}
setup_initial_schema(session, datas, tables)
puts ("hoge")
