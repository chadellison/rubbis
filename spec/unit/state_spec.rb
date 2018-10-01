require 'spec_helper'
require 'rubbis/state'

describe Rubbis::State, :unit do
  let(:state) { described_class.new }
  describe '#set' do
    it 'sets a value' do
      expect(state.set('abc', '123')).to eq :ok
      expect(state.get('abc')).to eq '123'
    end

    it 'does not overwrite an existing value with NX' do
      expect(state.set('abc', '123', 'NX')).to eq :ok
      expect(state.set('abc', '456', 'NX')).to be_nil
      expect(state.get('abc')).to eq '123'
    end

    it 'does not overwrite an existing value with XX' do
      expect(state.set('abc', '123', 'XX')).to be_nil
      state.set('abc', '123')
      expect(state.set('abc', '456', 'XX')).to eq :ok
      expect(state.get('abc')).to eq '456'
    end

    it 'returns error for wrong number of arguments' do
      expect(state.set('abc')).to eq Rubbis::Error.incorrect_args('set')
    end
  end
end
