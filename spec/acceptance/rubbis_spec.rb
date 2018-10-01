require 'redis'
require 'server'

TEST_PORT = 6380

describe 'Rubbis', :acceptance do
  it 'responds to ping' do
    with_server do
      c = client
      c.without_reconnect do
        expect(c.ping).to eq "PONG"
        expect(c.ping).to eq "PONG"
      end
    end
  end

  it 'echos messages' do
    with_server do
      expect(client.echo("hello\nthere")).to eq "hello\nthere"
    end
  end

  it 'supports multiple clients at a time' do
    with_server do
      expect(client.echo("hello\nthere")).to eq "hello\nthere"
      expect(client.echo("hello\nthere")).to eq "hello\nthere"
    end
  end

  it 'gets and sets values' do
    with_server do
      expect(client.get('abc')).to be_nil
      expect(client.set('abc', '123')).to eq 'OK'
      expect(client.get('abc')).to eq '123'
    end
  end

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
