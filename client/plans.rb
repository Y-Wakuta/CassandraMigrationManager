class TimeDependPlan
  attr_accessor :plans, :query

  def initialize(plans, query)
    @plans = []
    plans.each do |p|
      @plans << Plan.new(p)
    end
    @query = query
  end

  class Plan
    attr_accessor :steps

    def initialize(steps)
      @steps = steps
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
end

class MigratePlan
  attr_accessor :start_timestep, :obsolete_plan, :new_plan
end
