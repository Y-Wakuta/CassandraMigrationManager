class Migrate

  # @param [Array[Array]]
  # @return [Array]
  def self.find_migration(plans)
    migration_plans = []
    (plans).each_cons(2).each_with_index do |(former_plan, nex_plan), timestep|
      next if former_plan.steps == nex_plan.steps
      migration_plans << MigratePlan.new(timestep, former_plan, nex_plan)
    end
    migration_plans
  end

  # @param [MigratePlan, CassandraManager, Array]
  def self.exec_migration(migrate_plan, cassandraManager, initial, plans_for_timestep)
    puts "\e[36m migrate from: #{migrate_plan.obsolete_plan.inspect} \e[0m"
    puts "\e[36m to: #{migrate_plan.new_plan.inspect} \e[0m"
    initial_param = migrate_plan.obsolete_plan.first_table.name.start_with?("users") ? initial[:users] : initial[:items]

    obsolete_data = migrate_plan.obsolete_plan.execute(initial_param, cassandraManager)
    migrate_plan.new_plan.steps.each do |table|
      unless cassandraManager.isExist(table)
        cassandraManager.createTable(table)
        cassandraManager.insert_by_cf(obsolete_data, table)
      end
      migrate_plan.obsolete_plan.steps.each do |table|
        next if plans_for_timestep.any? do |plan|
          plan.steps.include? table
        end
        cassandraManager.dropTable(table)
      end
    end
  end
end
