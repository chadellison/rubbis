module Rubbis
  Error = Struct.new(:message) do
    def self.incorrect_args(cmd)
      new "wrong number of arguments for '#{cmd}' command"
    end

    def self.unknown_cmd(cmd)
      new "unknown command '#{cmd}'"
    end

    def self.type_error
      new "wrong type for command"
    end
  end

  class State
    def initialize
      @data = {}
    end

    def self.valid_command?(cmd)
      @valid_commands ||= Set.new(
        public_instance_methods(false).map(&:to_s) - ['apply_command']
      )
      @valid_commands.include?(cmd)
    end

    def apply_command(cmd)
      unless State.valid_command?(cmd[0])
        return Error.unknown_cmd(cmd[0])
      end

      public_send *cmd
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

    def hset(hash, key, value)
      data[hash] ||= {}
      data[hash][key] = value
      :ok
    end

    def hget(hash, key)
      data[hash][key]
    end

    def hmget(hash, *keys)
      existing = data.fetch(hash, {})
      if existing.is_a? Hash
        existing.values_at(*keys)
      else
        Error.type_error
      end
    end

    def hincrby(hash, key, amount)
      existing = data[hash][key]
      data[hash][key] = existing.to_i + amount.to_i
    end

    private

    attr_reader :data
  end
end
