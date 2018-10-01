module Rubbis
  Error = Struct.new(:message) do
    def self.incorrect_args(cmd)
      new "wrong number of arguments for '#{cmd}' command"
    end
  end

  class State
    attr_reader :data

    def initialize
      @data = {}
    end

    def set(*args)
      key, value, modifier = *args

      return Error.incorrect_args('set') unless key && value

      nx = modifier == 'NX'
      xx = modifier == 'XX'
      exists = data.has_key?(key)

      if (!nx && !xx) || (nx && !exists) || (xx && exists)
        data[key] = value
        :ok
      end
    end

    def get(key)
      data[key]
    end
  end
end
