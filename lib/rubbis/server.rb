require 'socket'
require 'rubbis/protocol'
require 'rubbis/state'

module Rubbis
  class Server
    attr_reader :port, :shutdown_pipe, :state, :clock

    class Clock
      def now
        Time.now.to_f
      end

      def sleep(x)
        ::Kernel.sleep x
      end
    end

    def initialize(port)
      @port = port
      @shutdown_pipe = IO.pipe
      @clock = Clock.new
      @state = State.new(@clock)
    end

    def shutdown
      shutdown_pipe[1].close
    end

    def listen
      readable = []
      clients = {}
      running = true
      timer_pipe = IO.pipe
      server = TCPServer.new(port)
      readable << server
      readable << shutdown_pipe[0]
      readable << timer_pipe[0]

      timer_thread = Thread.new do
        begin
          while running
            clock.sleep(0.1)
            timer_pipe[1].write('.')
          end
        rescue Errno::EPIPE
        end
      end

      while running
        ready_to_read = IO.select(readable + clients.keys).first

        ready_to_read.each do |socket|
          case socket
          when server
            child_socket = socket.accept
            clients[child_socket] = Handler.new(child_socket)
          when shutdown_pipe[0]
            running = false
          when timer_pipe[0]
            state.expire_keys!
          else
            begin
              clients[socket].process!(@state)
            rescue EOFError
              clients.delete(socket)
              socket.close
            end
          end
        end
      end
    ensure
      running = false
      (readable + clients.keys).each do |socket|
        socket.close
      end
      timer_pipe[0].close if timer_pipe
      timer_thread.join if timer_thread
    end
  end

  class Handler
    attr_reader :client, :buffer

    def initialize(socket)
      @client = socket
      @buffer = ''
    end

    def process!(state)
      buffer << client.read_nonblock(1024)

      cmds, processed = unmarshal(buffer)
      @buffer = buffer[processed..-1]

      cmds.each do |cmd|
        response = case cmd[0].to_s.downcase
        when 'ping' then :pong
        when 'echo' then cmd[1]
        else state.apply_command(cmd)
        end

        client.write Rubbis::Protocol.marshal(response)
      end
    end

    def unmarshal(data)
      io = StringIO.new(data)
      result = []
      processed = 0

      begin
        loop do
          header = safe_readline(io)
          raise ProtocolError unless header[0] == '*'

          n = header[1..-1].to_i

          result << n.times.map do
            raise ProtocolError unless io.readpartial(1) == '$'

            length = safe_readline(io).to_i
            safe_readpartial(io, length).tap do
              safe_readline(io)
            end
          end

          processed = io.pos
        end
      rescue ProtocolError
        processed = io.pos
      rescue EOFError
        # Incomplete command, ignore
      end

      [result, processed]
    end

    def safe_readline(io)
      io.readline("\r\n").tap do |line|
        raise EOFError unless line.end_with?("\r\n")
      end
    end

    def safe_readpartial(io, length)
      io.readpartial(length).tap do |data|
        raise EOFError unless data.length == length
      end
    end
  end

  class ProtocolError < RuntimeError
  end
end
