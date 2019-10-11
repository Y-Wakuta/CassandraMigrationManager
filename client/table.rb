class Table
  attr_accessor :name, :insert, :create, :fields

  def initialize(name, create, insert)
    @name = name
    @create = create
    @insert = insert
    @fields = []
  end
end

class Field
  attr_accessor :name
  attr_reader :type

  def type=(type)
    if type == Cassandra::Uuid
      @type = Cassandra::Uuid
    end

  end
end

