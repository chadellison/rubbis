require 'redis'
require 'rubbis/server'

module AcceptanceHelpers
  def client
    Redis.new(host: 'localhost', port: TEST_PORT)
  end

  def with_server
    server = nil
    server_thread = Thread.new do
      server = Rubbis::Server.new(TEST_PORT)
      server.listen
    end

    wait_for_open_port(TEST_PORT)

    yield
  ensure
    server.shutdown if server
  end

  def wait_for_open_port(port)
    time = Time.now
    while !check_port(port) && 1 > Time.now - time
      sleep 0.01
    end

    raise TimeoutError unless check_port(port)
  end

  def check_port(port)
    `nc -z localhost #{port}`
    $?.success?
  end
end

class FakeClock
  def initialize()
    @t = 0
  end

  def now
    @t
  end

  def sleep(t)
    @t += t
  end
end

TEST_PORT = 6380
RSpec.configure do |c|
  c.include AcceptanceHelpers, acceptance: true
end
