require 'spec_helper'

describe 'Rubbis', :acceptance do
  it 'actively expires keys' do
    with_server do
      n = 10
      n.times do |x|
        client.set("keep#{x}", '123')
        client.set("expire#{x}", '123')
        client.pexpire("expire#{x}", rand(600))
      end

      condition = -> {
        client.keys('*').count { |x| x.start_with?('expire') } == 0
      }
      start_time = Time.now
      while !condition.() && Time.now < start_time + 2
        sleep 0.01
      end

      expect(condition.()).to be true
      expect(client.keys('*').size).to eq(n)
    end
  end
end
