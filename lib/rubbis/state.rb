module Rubbis
  class State
    attr_reader :data

    def initialize
      @data = {}
    end

    def set(key, value)
      data[key] = value
      :ok
    end

    def get(key)
      data[key]
    end
  end
end
