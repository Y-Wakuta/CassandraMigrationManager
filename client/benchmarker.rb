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
    if t.name == "users_id" or t.name == "users_rating"
      datas[:users].to_a.each do |user|
        b.add(prepared, arguments: [
            Cassandra::Uuid.new(user['id']),
            user['firstname'],
            user['lastname'],
            user['nickname'],
            user['password'],
            user['email'],
            user['rating'],
            user['balance'],
            user['creation_date']
        ])
      end
    elsif t.name == "users_rating_secondary"
      datas[:users].to_a.each do |user|
        b.add(prepared, arguments: [
            user['rating'],
            Cassandra::Uuid.new(user['id'])
        ])
      end
    elsif t.name == "items_id" or t.name == "items_quantity"
      datas[:items].to_a.each do |item|
        b.add prepared, arguments: [
            Cassandra::Uuid.new(item['id']),
            item['name'],
            item['description'],
            item['initial_price'],
            item['quantity'],
            item['reserve_price'],
            item['buy_now'],
            item['nb_of_bids'],
            item['max_bid'],
            item['start_date'],
            item['end_date'],
        ]
      end
    elsif t.name == "items_quantity_secondary"
      datas[:items].to_a.each do |item|
        b.add prepared, arguments: [
            item['quantity'],
            Cassandra::Uuid.new(item['id'])
        ]
      end
    end
  end
  session.execute(batch)
end

table_names = [["users_id", "users_rating_secondary", "items_id", "items_quantity"],
               [ "users_id", "users_rating_secondary", "items_id","items_quantity"],
               [ "users_id", "users_rating", "items_id", "items_quantity_secondary"]]

users, items = get_data_from_mysql()
tables = Tables.new.tables
session = setup_cassandra tables

datas = {
    users: users,
    items: items
}
setup_initial_schema(session, datas, tables, table_names)
