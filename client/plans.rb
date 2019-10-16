class TimeDependPlan
  attr_accessor :plans, :query

  def initialize(plans, query)
    @plans = plans
    @query = query
  end
end

class Plan
  attr_accessor :steps

  def initialize(steps)
    @steps = steps
  end
end

class Step
  attr_accessor :cf_name, :inner_query

  def initialize(cf_name, inner_query)
    @cf_name = cf_name
    @inner_query = inner_query
  end
end

class MigratePlan
  attr_accessor :start_timestep, :obsolete_plan, :new_plan
end
