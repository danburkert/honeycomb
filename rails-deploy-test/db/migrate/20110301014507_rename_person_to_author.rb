class RenamePersonToAuthor < ActiveRecord::Migration
  def self.up
    rename_column :comments, :person_id, :author_id
    rename_column :posts, :person_id, :author_id
  end

  def self.down
    rename_column :comments, :author_id, :person_id
    rename_column :posts, :author_id, :person_id
  end
end
