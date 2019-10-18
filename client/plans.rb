class TimeDependPlan
  attr_accessor :tables, :plans

  def initialize(tables, query)
    @plans = []
    @tables = tables
    tables.each do |t|
      @plans << Plan.new(t)
    end
    @query = query
  end
end

class Plan
  attr_accessor :steps, :first_table

  def initialize(steps)
    @first_table = steps.first
    @steps = steps
  end

  def inspect
    @steps.map{|s| s.name}.join(" <-> ")
  end

  def execute(initial_param, cassandraManager)
    param = initial_param
    res = []
    @steps.map{|step_name| cassandraManager.gen_step(step_name)}.each do |step|
      tmp = step.call(param)
      param = tmp
      res << tmp
    end
    res.last # result of last step is required for the query
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
