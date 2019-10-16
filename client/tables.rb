require 'cassandra'
require_relative './table'

class Tables
    attr_accessor :tables

    def initialize
        @table_names = %w(users_id users_rating users_rating_secondary items_id items_quantity items_quantity_secondary)
        fields = {}
        @table_names.each do |table|
            fields[table] = {} if fields[table].nil?
            fields[table][:partition_keys] = Set.new if fields[table][:partition_keys].nil?
            fields[table][:clustering_keys] = Set.new if fields[table][:clutering_keys].nil?
            fields[table][:extras] = Set.new if fields[table][:extras].nil?
        end

        fields["users_id"][:partition_keys] << Field.new(:id, "UUID")
        fields["users_id"][:extras] << Field.new(:firstname, "TEXT")
        fields["users_id"][:extras] << Field.new(:lastname, "TEXT")
        fields["users_id"][:extras] << Field.new(:nickname, "TEXT")
        fields["users_id"][:extras] << Field.new(:password, "TEXT")
        fields["users_id"][:extras] << Field.new(:email, "TEXT")
        fields["users_id"][:extras] << Field.new(:rating, "INT")
        fields["users_id"][:extras] << Field.new(:balance, "DOUBLE")
        fields["users_id"][:extras] << Field.new(:creation_date, "TIMESTAMP")

        fields["users_rating"][:partition_keys] << Field.new(:rating, "INT")
        fields["users_rating"][:clustering_keys] << Field.new(:id, "UUID")
        fields["users_rating"][:extras] << Field.new(:firstname, "TEXT")
        fields["users_rating"][:extras] << Field.new(:lastname, "TEXT")
        fields["users_rating"][:extras] << Field.new(:nickname, "TEXT")
        fields["users_rating"][:extras] << Field.new(:password, "TEXT")
        fields["users_rating"][:extras] << Field.new(:email, "TEXT")
        fields["users_rating"][:extras] << Field.new(:balance, "DOUBLE")
        fields["users_rating"][:extras] << Field.new(:creation_date, "TIMESTAMP")

        fields["users_rating_secondary"][:partition_keys] << Field.new(:rating, "INT")
        fields["users_rating_secondary"][:clustering_keys] << Field.new(:id, "UUID")

        fields["items_id"][:partition_keys] << Field.new(:id, "UUID")
        fields["items_id"][:extras] << Field.new(:name, "TEXT")
        fields["items_id"][:extras] << Field.new(:description, "TEXT")
        fields["items_id"][:extras] << Field.new(:initial_price, "DOUBLE")
        fields["items_id"][:extras] << Field.new(:quantity, "INT")
        fields["items_id"][:extras] << Field.new(:reserve_price, "DOUBLE")
        fields["items_id"][:extras] << Field.new(:buy_now, "DOUBLE")
        fields["items_id"][:extras] << Field.new(:nb_of_bids, "INT")
        fields["items_id"][:extras] << Field.new(:max_bid, "DOUBLE")
        fields["items_id"][:extras] << Field.new(:start_date, "TIMESTAMP")
        fields["items_id"][:extras] << Field.new(:end_date, "TIMESTAMP")

        fields["items_quantity"][:partition_keys] << Field.new(:quantity, "INT")
        fields["items_quantity"][:clustering_keys] << Field.new(:id, "UUID")
        fields["items_quantity"][:extras] << Field.new(:name, "TEXT")
        fields["items_quantity"][:extras] << Field.new(:description, "TEXT")
        fields["items_quantity"][:extras] << Field.new(:initial_price, "DOUBLE")
        fields["items_quantity"][:extras] << Field.new(:reserve_price, "DOUBLE")
        fields["items_quantity"][:extras] << Field.new(:buy_now, "DOUBLE")
        fields["items_quantity"][:extras] << Field.new(:nb_of_bids, "INT")
        fields["items_quantity"][:extras] << Field.new(:max_bid, "DOUBLE")
        fields["items_quantity"][:extras] << Field.new(:start_date, "TIMESTAMP")
        fields["items_quantity"][:extras] << Field.new(:end_date, "TIMESTAMP")

        fields["items_quantity_secondary"][:partition_keys] << Field.new(:quantity, "INT")
        fields["items_quantity_secondary"][:clustering_keys] << Field.new(:id, "UUID")

        @tables = []
        @tables << Table.new("users_id", fields, :users)
        @tables << Table.new("users_rating", fields, :users)
        @tables << Table.new("users_rating_secondary", fields, :users)
        @tables << Table.new("items_id", fields, :items)
        @tables << Table.new("items_quantity", fields, :items)
        @tables << Table.new("items_quantity_secondary", fields, :items)
    end
end