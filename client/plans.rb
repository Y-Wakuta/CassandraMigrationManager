require 'benchmark'

class TimeDependPlan
  attr_accessor :tables, :plans

  def initialize(tables, query)
    @plans = []
    @tables = tables
    @migration_interval = 10
    tables.each_with_index do |t, timestep|
      @plans << Plan.new(t, query, timestep, @migration_interval)
    end
    @query = query
  end
end

class Plan
  attr_accessor :steps, :first_table, :query

  def initialize(steps, query, timestep, migration_interval)
    @first_table = steps.first
    @steps = steps
    @query = query[:query]
    @frequency = query[:frequency][timestep]
    @migration_interval = migration_interval
  end

  def inspect
    @steps.map{|s| s.name}.join(" <-> ")
  end

  def execute_bench(initial_param)
    cassandraManager = CassandraManager.new('rubis')
    interval do
      exec_time = []
      res = []
      Benchmark.bm do |x|
        param = initial_param
        exec_time << x.report(""){
          @steps.map{|step_name| cassandraManager.gen_step(step_name)}.each do |step|
            tmp = step.call(param)
            param = tmp
            res << tmp
          end
          res.last # result of last step is required for the query
        }
      end
      exec_time
    end
  end

  def execute(initial_param)
    cassandraManager = CassandraManager.new('rubis')
    res = []
    param = initial_param
    @steps.map{|step_name| cassandraManager.gen_step(step_name)}.each do |step|
      tmp = step.call(param)
      param = tmp
      res << tmp
    end
    res.last # result of last step is required for the query
  end

  def interval
    interval_in_second = 1.0 / @frequency
    last = Time.now
    start = last
    res = []
    while true and (Time.now - start < @migration_interval - 1)
      res << yield
      now = Time.now
      _next = [last + interval_in_second, now].max
      sleep(_next - now)
      last = _next
    end
    res
  end
end

class MigratePlan
  attr_accessor :start_timestep, :end_timestep, :obsolete_plan, :new_plan

  # @param [Integer, Plan, Plan]
  # @return [void]
  def initialize(start_timestep, obsolete_plan, new_plan)
    @start_timestep = start_timestep
    @end_timestep = start_timestep + 1
    @obsolete_plan = obsolete_plan
    @new_plan = new_plan
  end
end
