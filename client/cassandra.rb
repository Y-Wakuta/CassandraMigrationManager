class CassandraManager
  def initialize(keyspace_name)
    @keyspace_name = keyspace_name
    cluster = Cassandra.cluster
    #cluster = Cassandra.cluster(hosts: ['cassandra'])
    @session = cluster.connect(keyspace_name)
    @session.execute("USE #{keyspace_name}")
  end

  def createTable(table)
    @session.execute(table.create)
  end

  def isExist(table)
    @session.execute("SELECT table_name FROM system_schema.tables").to_a.map{|b| b.map{|_, v| v }}.flatten(1).include? table.name
  end

  def clearKeyspace
    tables = @session.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name = '#{@keyspace_name}'").to_a
    tables.map{|h| h.values}.flatten(1).each do |table|
      @session.execute("DROP TABLE #{table}")
    end
  end

  def dropTable(table)
    @session.execute("DROP TABLE IF EXISTS #{table.name}")
  end

  def executeQuery(query)
    @session.execute(query).to_a
  end

  def insert_data(data, table)
    prepared = @session.prepare table.insert
    data[table.entity].to_a.each_slice(500).each do |records|
      records.each do |r|
        batch = @session.batch do |b|
          values = table.all_fields.sort_by { |f | f.name}.map do |field|
            field.name == :id ? Cassandra::Uuid.new(r[field.name.to_s])
                : r[field.name.to_s]
          end.to_a
          b.add(prepared, arguments: values)
        end
        @session.execute(batch)
      end
    end
  end

  def insert_by_cf(data, table)
    prepared = @session.prepare table.insert
    batch = @session.batch do |b|
      data.to_a.each do |d|
        values = table.all_fields.sort_by { |f | f.name}.map do |field|
          d[field.name.to_s]
        end.to_a
        b.add(prepared, arguments: values)
      end
    end
    @session.execute(batch)
  end

  def initKeyspace(cfs, data)
    clearKeyspace
    cfs.each do |cf|
      createTable(cf)
      insert_data(data, cf)
    end
  end

  def gen_step(table)
    Proc.new do |params|
      prepared = @session.prepare table.select
      fields = (table.partition_keys + table.clustering_keys).sort_by { |f | f.name }
      params.map do |param|
        args = fields.map{|f| param[f.name.to_s]}
        @session.execute(prepared, arguments: args ).to_a
      end.flatten(1)
    end
  end
end
