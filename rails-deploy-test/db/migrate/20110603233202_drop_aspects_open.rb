class DropAspectsOpen < ActiveRecord::Migration
  require  File.join(Dir.pwd,"db/migrate/20110202015222_add_open_to_aspects.rb")
  def self.up
    AddOpenToAspects.down
  end

  def self.down
    AddOpenToAspects.up
  end
end
