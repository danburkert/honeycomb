class CreateRoles < ActiveRecord::Migration
  def change
    create_table :roles, :options => "engine=Honeycomb" do |t|
      t.integer :person_id
      t.string :name

      t.timestamps
    end
  end
end
