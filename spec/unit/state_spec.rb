require 'spec_helper'
require 'rubbis/state'

describe Rubbis::State, :unit do
  let(:state) { described_class.new }
  describe '#set' do
    it 'sets a value' do
      expect(state.set('abc', '123')).to eq :ok
      expect(state.get('abc')).to eq '123'
    end
  end
end
