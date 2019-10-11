require_relative './table'

class Tables
  attr_accessor :tables

  def initialize
    @cf_users_id_create = <<-TABLE_CQL
      CREATE TABLE rubis.users_id (
        id UUID,
        firstname TEXT,
        lastname TEXT,
        nickname TEXT,
        password TEXT,
        email TEXT,
        rating INT,
        balance double,
        creation_date timestamp,
        PRIMARY KEY (id)
      )
    TABLE_CQL

    @cf_users_id_insert = <<-TABLE_CQL
      INSERT INTO rubis.users_id (id, firstname, lastname, nickname, password, email, rating, balance, creation_date) 
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)

    TABLE_CQL

    @cf_users_rating_secondary_create = <<-TABLE_CQL
      CREATE TABLE rubis.users_rating_secondary (
        rating INT,
        id UUID,
        PRIMARY KEY (rating, id)
      )
    TABLE_CQL

    @cf_users_rating_secondary_insert = <<-TABLE_CQL
    INSERT INTO rubis.users_rating_secondary (rating, id) VALUES (?, ?)
    TABLE_CQL


    @cf_users_rating_create = <<-TABLE_CQL
      CREATE TABLE rubis.users_rating (
        id UUID,
        firstname TEXT,
        lastname TEXT,
        nickname TEXT,
        password TEXT,
        email TEXT,
        rating INT,
        balance DOUBLE,
        creation_date TIMESTAMP,
        PRIMARY KEY (rating, id)
      )
    TABLE_CQL

    @cf_users_rating_insert = <<-TABLE_CQL
    INSERT INTO rubis.users_rating (id, firstname, lastname, nickname, password, email, rating, balance, creation_date)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    TABLE_CQL

    @cf_items_id_create = <<-TABLE_CQL
      CREATE TABLE rubis.items_id (
        id UUID,
        name TEXT,
        description TEXT, 
        initial_price DOUBLE, 
        quantity INT, 
        reserve_price DOUBLE, 
        buy_now DOUBLE, 
        nb_of_bids INT, 
        max_bid DOUBLE, 
        start_date TIMESTAMP,
        end_date TIMESTAMP,
        PRIMARY KEY (id)
      )
    TABLE_CQL

    @cf_items_id_insert = <<-TABLE_CQL
    INSERT INTO rubis.items_id (id, name, description, initial_price, quantity, reserve_price, buy_now, nb_of_bids, max_bid, start_date, end_date)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    TABLE_CQL

    @cf_items_quantity_secondary_create = <<-TABLE_CQL
      CREATE TABLE rubis.items_quantity_secondary (
        quantity INT,
        id UUID,
        PRIMARY KEY (quantity, id)
      )
    TABLE_CQL

    @cf_items_quantity_secondary_insert = <<-TABLE_CQL
    INSERT INTO rubis.items_quantity_secondary (quantity, id) VALUES (?, ?)
    TABLE_CQL

    @cf_items_quantity_create = <<-TABLE_CQL
      CREATE TABLE rubis.items_quantity (
        id UUID,
        name TEXT,
        description TEXT,
        initial_price DOUBLE,
        quantity INT,
        reserve_price DOUBLE,
        buy_now DOUBLE,
        nb_of_bids INT,
        max_bid DOUBLE,
        start_date TIMESTAMP,
        end_date TIMESTAMP,
        PRIMARY KEY (quantity, id)
      )
    TABLE_CQL

    @cf_items_quantity_insert = <<-TABLE_CQL
    INSERT INTO rubis.items_quantity (id, name, description, initial_price, quantity, reserve_price, buy_now, nb_of_bids, max_bid, start_date, end_date)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    TABLE_CQL

    @tables = []
    @tables << Table.new("users_id", @cf_users_id_create, @cf_users_id_insert)
    @tables << Table.new("users_rating", @cf_users_rating_create, @cf_users_rating_insert)
    @tables << Table.new("users_rating_secondary", @cf_users_rating_secondary_create, @cf_users_rating_secondary_insert)
    @tables << Table.new("items_id", @cf_items_id_create, @cf_items_id_insert)
    @tables << Table.new("items_quantity", @cf_items_quantity_create, @cf_items_quantity_insert)
    @tables << Table.new("items_quantity_secondary", @cf_items_quantity_secondary_create, @cf_items_quantity_secondary_insert)
  end
end