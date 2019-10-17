class Table
  attr_accessor :name, :insert, :create, :select, :entity, :partition_keys, :clustering_keys, :extras, :all_fields

  def initialize(name, fields, entity)
    @name = name
    @partition_keys = fields[name][:partition_keys]
    @clustering_keys = fields[name][:clustering_keys]
    @extras = fields[name][:extras]
    @entity = entity

    @all_fields = @partition_keys + @clustering_keys + @extras
    @create = gen_create
    @insert = gen_insert
    @select = gen_select
  end

  def gen_select
      "SELECT * FROM rubis.#{@name} WHERE #{(@partition_keys + @clustering_keys).sort_by { |f | f.name}.map{|f| "#{f.name.to_s} = ? " }.join("AND ")}"
  end

  def gen_insert
    <<-TABLE_CQL
      INSERT INTO rubis.#{@name} (#{@all_fields.sort_by { |f | f.name}.map{|f| f.name.to_s}.join(", ")}) VALUES (#{(["?"] * @all_fields.size).join(", ")})
    TABLE_CQL
  end

  def gen_create
    <<-TABLE_CQL
       CREATE TABLE rubis.#{@name} (
        #{@all_fields.sort_by{|f| f.name}
                     .map{|f| f.name.to_s + " " + f.type }
                     .join(", \n")},
        PRIMARY KEY (#{@partition_keys.sort_by{|f| f.name}
                                      .map{|f| f.name.to_s}
                                      .join(", ")}                    
                     #{"," if @clustering_keys.size > 0}
                     #{@clustering_keys.sort_by{|f| f.name}
                                       .map{|f| f.name.to_s}
                                       .join(", ")}))
    TABLE_CQL
  end
end

class Field
  attr_accessor :name, :type

  def initialize(name, type)
    @name = name
    @type = type
  end
end

