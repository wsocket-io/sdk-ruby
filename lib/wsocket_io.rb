# frozen_string_literal: true

# wSocket Ruby SDK — Realtime Pub/Sub client with Presence, History, and Push.
#
# Usage:
#   client = WSocketIO::Client.new('ws://localhost:9001', 'your-api-key')
#   client.connect
#   ch = client.pubsub.channel('chat')
#   ch.subscribe { |data, meta| puts data }
#   ch.publish({ text: 'hello' })

require 'json'
require 'websocket-client-simple'
require 'net/http'
require 'uri'
require 'securerandom'
require 'base64'

module WSocketIO
  # ─── Types ──────────────────────────────────────────────────

  MessageMeta = Struct.new(:id, :channel, :timestamp, keyword_init: true)

  PresenceMember = Struct.new(:client_id, :data, :joined_at, keyword_init: true) do
    def initialize(client_id: '', data: nil, joined_at: 0)
      super
    end
  end

  HistoryMessage = Struct.new(:id, :channel, :data, :publisher_id, :timestamp, :sequence, keyword_init: true) do
    def initialize(id: '', channel: '', data: nil, publisher_id: '', timestamp: 0, sequence: 0)
      super
    end
  end

  HistoryResult = Struct.new(:channel, :messages, :has_more, keyword_init: true) do
    def initialize(channel: '', messages: [], has_more: false)
      super
    end
  end

  Options = Struct.new(:auto_reconnect, :max_reconnect_attempts, :reconnect_delay, :token, :recover, keyword_init: true) do
    def initialize(auto_reconnect: true, max_reconnect_attempts: 10, reconnect_delay: 1.0, token: nil, recover: true)
      super
    end
  end

  # ─── Presence ───────────────────────────────────────────────

  class Presence
    def initialize(channel_name, send_fn)
      @channel_name = channel_name
      @send_fn = send_fn
      @enter_cbs = []
      @leave_cbs = []
      @update_cbs = []
      @members_cbs = []
    end

    def enter(data: nil)
      @send_fn.call({ action: 'presence.enter', channel: @channel_name, data: data })
      self
    end

    def leave
      @send_fn.call({ action: 'presence.leave', channel: @channel_name })
      self
    end

    def update(data)
      @send_fn.call({ action: 'presence.update', channel: @channel_name, data: data })
      self
    end

    def get
      @send_fn.call({ action: 'presence.get', channel: @channel_name })
      self
    end

    def on_enter(&block) = (@enter_cbs << block; self)
    def on_leave(&block) = (@leave_cbs << block; self)
    def on_update(&block) = (@update_cbs << block; self)
    def on_members(&block) = (@members_cbs << block; self)

    def handle_event(action, data)
      case action
      when 'presence.enter'
        m = parse_member(data)
        @enter_cbs.each { |cb| cb.call(m) }
      when 'presence.leave'
        m = parse_member(data)
        @leave_cbs.each { |cb| cb.call(m) }
      when 'presence.update'
        m = parse_member(data)
        @update_cbs.each { |cb| cb.call(m) }
      when 'presence.members'
        members = (data['members'] || []).map { |d| parse_member(d) }
        @members_cbs.each { |cb| cb.call(members) }
      end
    end

    private

    def parse_member(d)
      PresenceMember.new(
        client_id: d['clientId'] || '',
        data: d['data'],
        joined_at: d['joinedAt'] || 0
      )
    end
  end

  # ─── Channel ────────────────────────────────────────────────

  class Channel
    attr_reader :name, :presence

    def initialize(name, send_fn)
      @name = name
      @send_fn = send_fn
      @message_cbs = []
      @history_cbs = []
      @presence = Presence.new(name, send_fn)
    end

    def subscribe(&callback)
      @message_cbs << callback if callback
      @send_fn.call({ action: 'subscribe', channel: @name })
      self
    end

    def unsubscribe
      @send_fn.call({ action: 'unsubscribe', channel: @name })
      @message_cbs.clear
      self
    end

    def publish(data, persist: nil)
      msg = { action: 'publish', channel: @name, data: data, id: SecureRandom.uuid }
      msg[:persist] = persist unless persist.nil?
      @send_fn.call(msg)
      self
    end

    def history(limit: nil, before: nil, after: nil, direction: nil)
      opts = { action: 'history', channel: @name }
      opts[:limit] = limit if limit
      opts[:before] = before if before
      opts[:after] = after if after
      opts[:direction] = direction if direction
      @send_fn.call(opts)
      self
    end

    def on_history(&block)
      @history_cbs << block
      self
    end

    def handle_message(data, meta)
      @message_cbs.each { |cb| cb.call(data, meta) }
    end

    def handle_history(result)
      @history_cbs.each { |cb| cb.call(result) }
    end
  end

  # ─── PubSub Namespace ──────────────────────────────────────

  class PubSubNamespace
    def initialize(client)
      @client = client
    end

    def channel(name)
      @client.channel(name)
    end
  end

  # ─── Push Client ────────────────────────────────────────────

  class PushClient
    def initialize(base_url:, token:, app_id:)
      @base_url = base_url
      @token = token
      @app_id = app_id
    end

    def register_fcm(device_token:, member_id:)
      post('register', { memberId: member_id, platform: 'fcm', subscription: { deviceToken: device_token } })
    end

    def register_apns(device_token:, member_id:)
      post('register', { memberId: member_id, platform: 'apns', subscription: { deviceToken: device_token } })
    end

    def send_to_member(member_id, payload:)
      post('send', { memberId: member_id, payload: payload })
    end

    def broadcast(payload:)
      post('broadcast', { payload: payload })
    end

    def unregister(member_id, platform: nil)
      body = { memberId: member_id }
      body[:platform] = platform if platform
      uri = URI("#{@base_url}/api/push/unregister")
      req = Net::HTTP::Delete.new(uri, headers)
      req.body = body.to_json
      Net::HTTP.start(uri.hostname, uri.port, use_ssl: uri.scheme == 'https') { |http| http.request(req) }
    end

    def delete_subscription(subscription_id)
      uri = URI("#{@base_url}/api/push/subscriptions/#{subscription_id}")
      req = Net::HTTP::Delete.new(uri, headers)
      Net::HTTP.start(uri.hostname, uri.port, use_ssl: uri.scheme == 'https') { |http| http.request(req) }
    end

    def add_channel(member_id, channel:)
      post('channels/add', { memberId: member_id, channel: channel })
    end

    def remove_channel(member_id, channel:)
      post('channels/remove', { memberId: member_id, channel: channel })
    end

    def get_vapid_key
      uri = URI("#{@base_url}/api/push/vapid-key")
      req = Net::HTTP::Get.new(uri, headers)
      resp = Net::HTTP.start(uri.hostname, uri.port, use_ssl: uri.scheme == 'https') { |http| http.request(req) }
      data = JSON.parse(resp.body)
      data['vapidPublicKey']
    end

    def list_subscriptions(member_id: nil, platform: nil, limit: nil)
      params = []
      params << "memberId=#{member_id}" if member_id
      params << "platform=#{platform}" if platform
      params << "limit=#{limit}" if limit
      qs = params.any? ? "?#{params.join('&')}" : ''
      uri = URI("#{@base_url}/api/push/subscriptions#{qs}")
      req = Net::HTTP::Get.new(uri, headers)
      resp = Net::HTTP.start(uri.hostname, uri.port, use_ssl: uri.scheme == 'https') { |http| http.request(req) }
      JSON.parse(resp.body)
    end

    private

    def headers
      { 'Authorization' => "Bearer #{@token}", 'X-App-Id' => @app_id, 'Content-Type' => 'application/json' }
    end

    def post(path, body)
      uri = URI("#{@base_url}/api/push/#{path}")
      req = Net::HTTP::Post.new(uri, headers)
      req.body = body.to_json
      Net::HTTP.start(uri.hostname, uri.port, use_ssl: uri.scheme == 'https') { |http| http.request(req) }
    end
  end

  # ─── Client ─────────────────────────────────────────────────

  class Client
    attr_reader :pubsub

    def initialize(url, api_key, options = Options.new)
      @url = url
      @api_key = api_key
      @options = options
      @channels = {}
      @subscribed_channels = {}
      @last_message_ts = 0
      @reconnect_attempts = 0
      @connected = false
      @ws = nil

      @on_connect_cbs = []
      @on_disconnect_cbs = []
      @on_error_cbs = []

      @pubsub = PubSubNamespace.new(self)
    end

    def on_connect(&block) = (@on_connect_cbs << block; self)
    def on_disconnect(&block) = (@on_disconnect_cbs << block; self)
    def on_error(&block) = (@on_error_cbs << block; self)
    def connected? = @connected

    def connect
      ws_url = @url.dup
      ws_url += @url.include?('?') ? '&' : '?'
      ws_url += "key=#{@api_key}"
      ws_url += "&token=#{@options.token}" if @options.token

      client = self
      @ws = WebSocket::Client::Simple.connect(ws_url) do |ws|
        ws.on :open do
          client.send(:handle_open)
        end

        ws.on :message do |msg|
          client.send(:handle_raw_message, msg.data)
        end

        ws.on :close do |e|
          client.send(:handle_close, e)
        end

        ws.on :error do |e|
          client.send(:handle_error, e)
        end
      end

      self
    end

    def disconnect
      @connected = false
      @ws&.close
    end

    def channel(name)
      @channels[name] ||= Channel.new(name, method(:send_msg))
    end

    def configure_push(base_url:, token:, app_id:)
      PushClient.new(base_url: base_url, token: token, app_id: app_id)
    end

    private

    def send_msg(msg)
      return unless @connected

      hash = stringify_keys(msg)
      @ws&.send(hash.to_json)
    end

    def handle_open
      @connected = true
      @reconnect_attempts = 0

      if @options.recover && !@subscribed_channels.empty? && @last_message_ts > 0
        resume_data = { channels: @subscribed_channels.keys, since: @last_message_ts }
        token = Base64.urlsafe_encode64(resume_data.to_json, padding: false)
        send_msg({ action: 'resume', token: token })
      else
        @subscribed_channels.each_key do |ch|
          send_msg({ action: 'subscribe', channel: ch })
        end
      end

      @on_connect_cbs.each(&:call)
    end

    def handle_raw_message(raw)
      msg = JSON.parse(raw)
      action = msg['action']
      return unless action

      channel_name = msg['channel']

      case action
      when 'message'
        ch = channel_name && @channels[channel_name]
        return unless ch

        ts = msg['timestamp']&.to_i || (Time.now.to_f * 1000).to_i
        @last_message_ts = ts if ts > @last_message_ts
        meta = MessageMeta.new(id: msg['id'] || '', channel: channel_name, timestamp: ts)
        ch.handle_message(msg['data'], meta)

      when 'subscribed'
        @subscribed_channels[channel_name] = true if channel_name

      when 'unsubscribed'
        @subscribed_channels.delete(channel_name) if channel_name

      when 'history'
        ch = channel_name && @channels[channel_name]
        return unless ch

        messages = (msg['messages'] || []).map do |m|
          HistoryMessage.new(
            id: m['id'] || '', channel: channel_name,
            data: m['data'], publisher_id: m['publisherId'] || '',
            timestamp: m['timestamp']&.to_i || 0,
            sequence: m['sequence']&.to_i || 0
          )
        end
        ch.handle_history(HistoryResult.new(channel: channel_name, messages: messages, has_more: msg['hasMore'] == true))

      when 'presence.enter', 'presence.leave', 'presence.update', 'presence.members'
        ch = channel_name && @channels[channel_name]
        return unless ch

        ch.presence.handle_event(action, msg)

      when 'error'
        err = msg['error'] || 'Unknown error'
        @on_error_cbs.each { |cb| cb.call(RuntimeError.new(err)) }
      end
    rescue StandardError => e
      @on_error_cbs.each { |cb| cb.call(e) }
    end

    def handle_close(_event)
      @connected = false
      @on_disconnect_cbs.each { |cb| cb.call(1000) }
      maybe_reconnect
    end

    def handle_error(error)
      @connected = false
      @on_error_cbs.each { |cb| cb.call(error) }
      maybe_reconnect
    end

    def maybe_reconnect
      return unless @options.auto_reconnect
      return if @reconnect_attempts >= @options.max_reconnect_attempts

      @reconnect_attempts += 1
      delay = @options.reconnect_delay * @reconnect_attempts
      Thread.new do
        sleep(delay)
        connect unless @connected
      end
    end

    def stringify_keys(hash)
      hash.transform_keys(&:to_s).transform_values do |v|
        v.is_a?(Hash) ? stringify_keys(v) : v
      end
    end
  end
end
