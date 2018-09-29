require 'socket'


module Rubbis
  class Server
    attr_reader :port

    def initialize(port)
      @port = port
    end

    def listen
      socket = TCPServer.new(port)
      loop do
        handle_client(socket.accept)
      end
    ensure
      socket.close if socket
    end

    def handle_client(client)
      loop do
        header = client.gets.to_s

        return unless header[0] == '*'

        num_args = header[1..-1].to_i

        cmd = num_args.times.map do
          len = client.gets[1..-1].to_i
          client.read(len + 2).chomp
        end

        response = case cmd[0].downcase
        when 'ping' then "+PONG\r\n"
        when 'echo' then "$#{cmd[1].length}\r\n#{cmd[1]}\r\n"
        end
        client.write response
      end
    ensure
      client.close
    end
  end
end
