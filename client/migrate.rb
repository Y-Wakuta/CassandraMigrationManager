class Migrate

  # @param [MigratePlan, CassandraManager, Array]
  def self.exec_migration(migrate_plan, cassandraManager, initial)
    puts "\e[36m migrate from: #{migrate_plan.obsolete_plan.inspect} \e[0m"
    puts "\e[36m to: #{migrate_plan.new_plan.inspect} \e[0m"

    obsolete_data = migrate_plan.obsolete_plan.execute(initial[migrate_plan.obsolete_plan.first_table])
    migrate_plan.new_plan.steps.each do |table|
      unless cassandraManager.isExist(table)
        cassandraManager.createTable(table)
        cassandraManager.insert_by_cf(obsolete_data, table)
      end
    end
  end

  def self.drop_obsolete_tables(migrate_plan, cassandraManager, plans_for_timestep)
    migrate_plan.obsolete_plan.steps.each do |table|
      next if plans_for_timestep.any? do |plan|
        plan.steps.include? table
      end
      cassandraManager.dropTable(table)
    end
  end
end
