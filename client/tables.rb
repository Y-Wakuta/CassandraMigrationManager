class Tables
  attr_reader :cf_users_id, :cf_users_rating_secondary, :cf_users_rating,
              :cf_items_id, :cf_items_quantity_secondary, :cf_items_quantity

  def initialize
    @cf_users_id = <<-TABLE_CQL
      CREATE TABLE rubis.users_id (
        id UUID,
        firstname TEXT,
        lastname TEXT,
        nickname TEXT,
        password TEXT,
        email TEXT,
        rating INT,
        balance double,
        creation_data date,
        PRIMARY KEY (id)
      )
    TABLE_CQL

    @cf_users_rating_secondary = <<-TABLE_CQL
      CREATE TABLE rubis.users_rating_secondary (
        rating INT,
        id UUID,
        PRIMARY KEY (rating, id)
      )
    TABLE_CQL

    @cf_users_rating = <<-TABLE_CQL
      CREATE TABLE rubis.users_rating (
        id UUID,
        firstname TEXT,
        lastname TEXT,
        nickname TEXT,
        password TEXT,
        email TEXT,
        rating INT,
        balance DOUBLE,
        creation_data DATE,
        PRIMARY KEY (rating, id)
      )
    TABLE_CQL

    @cf_items_id = <<-TABLE_CQL
      CREATE TABLE rubis.items_id (
        id UUID,
        name TEXT,
        description TEXT, 
        initial_price INT, 
        quantity INT, 
        reserve_price INT, 
        buy_now INT, 
        nb_of_bids INT, 
        max_bid INT, 
        start_date DATE,
        end_date DATE,
        PRIMARY KEY (id)
      )
    TABLE_CQL

    @cf_items_quantity_secondary = <<-TABLE_CQL
      CREATE TABLE rubis.items_quanaity_secondary (
        quantity INT,
        id UUID,
        PRIMARY KEY (quantity, id)
      )
    TABLE_CQL

    @cf_items_quantity = <<-TABLE_CQL
      CREATE TABLE rubis.items_quantity (
        quantity INT,
        id UUID,
        name TEXT,
        description TEXT,
        initial_price INT,
        reserve_price INT,
        buy_now INT,
        nb_of_bids INT,
        max_bid INT,
        start_date DATE,
        end_date DATE,
        PRIMARY KEY (quantity, id)
      )
    TABLE_CQL
  end
end
