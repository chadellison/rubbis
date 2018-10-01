module Rubbis
  class Protocol
    def self.marshal(ruby)
      case ruby
      when Symbol then "+#{ruby.to_s.upcase}\r\n"
      when nil then "$-1\r\n"
      when String then "$#{ruby.length}\r\n#{ruby}\r\n"
      when Error then "-ERR #{ruby.message}\r\n"
      else raise "Don't know how to marshal #{ruby}"
      end
    end
  end
end
