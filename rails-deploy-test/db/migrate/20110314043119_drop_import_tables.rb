require File.join(Dir.pwd, 'db/migrate/20110105051803_create_import_tables.rb')
class DropImportTables < ActiveRecord::Migration
  def self.up
    CreateImportTables.down
  end

  def self.down
    CreateImportTables.up
  end
end
