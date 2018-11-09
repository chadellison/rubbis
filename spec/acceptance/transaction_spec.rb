require 'spec_helper'

describe 'Rubbis', :acceptance do
  it 'handles transactions' do
    with_server do
      c = client

      result = c.multi do
        c.set('abc', '123')
        c.get('abc')
      end

      expect(result).to eq %w(OK 123)

      begin
        c.multi do
          c.set('abc', '456')
          raise
        end
      rescue
      end

      expect(c.get('abc')).to eq '123'
    end
  end

  it 'supports WATCH' do
    with_server do
      c1 = client
      c2 = client

      c1.set('abc', 1)
      c1.watch('abc')
      c1.watch('def')

      x = c1.get('abc')

      c2.set('abc', '10')

      c1.multi do
        c1.set('abc', x.to_i + 1)
      end

      # expect(c1.get('abc')).to eq '10'
      #
      # c1.watch('abc')
      # c1.watch('def')
      #
      # x = c1.get('abc')
      #
      # c1.multi do
      #   c1.set('abc', x.to_i + 1)
      # end
      #
      # expect(c1.get('abc')).to eq '11'
    end
  end
end
