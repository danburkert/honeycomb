class MoveRecentlyHiddenPostsToUser < ActiveRecord::Migration
  def self.up
    add_column :users, :hidden_shareables, :text
  end

  def self.down
    remove_column :users, :hidden_shareables
  end
end
