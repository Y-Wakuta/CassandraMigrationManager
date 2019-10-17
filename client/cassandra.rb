class CassandraManager
  def initialize(keyspace_name)
    cluster = Cassandra.cluster
    cluster.each_host do |host|
      puts "Host #{host.ip}: id=#{host.id} datacenter=#{host.datacenter} rack=#{host.rack}"
    end
    @session = cluster.connect(keyspace_name)
    @session.execute("USE #{keyspace_name}")
  end

  def createTable(table)
    @session.execute(table.create)
  end

  def dropTable(table)
    @session.execute("DROP TABLE IF EXISTS #{table.name}")
  end

  def executeQuery(query)
    @session.execute(query).to_a
  end

  def insert_data(data, table)
    prepared = @session.prepare table.insert
    batch = @session.batch do |b|
      data[table.entity].to_a.each do |data|
        b.add(prepared, arguments:
            table.all_fields.sort_by { |f | f.name}.map do |field|
              field.name == :id ? Cassandra::Uuid.new(data[field.name.to_s])
                  : data[field.name.to_s]
            end.to_a)
      end
    end
    @session.execute(batch)
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
