# wSocket SDK for Ruby

Official Ruby SDK for [wSocket](https://wsocket.io) — realtime pub/sub, presence, history, and push notifications.

[![Gem Version](https://badge.fury.io/rb/wsocket-io.svg)](https://rubygems.org/gems/wsocket-io)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

## Installation

Add to your Gemfile:

```ruby
gem 'wsocket-io'
```

Then run:

```bash
bundle install
```

Or install directly:

```bash
gem install wsocket-io
```

## Quick Start

```ruby
require 'wsocket_io'

client = WSocketIO::Client.new('wss://node00.wsocket.online', 'your-api-key')

client.on_connect { puts 'Connected!' }

client.connect

channel = client.pubsub.channel('chat')

channel.subscribe do |data, meta|
  puts "Received: #{data}"
end

channel.publish({ text: 'Hello from Ruby!' })
```

## Presence

```ruby
channel = client.pubsub.channel('room')

channel.presence.enter(data: { name: 'Alice' })

channel.presence.on_enter do |member|
  puts "#{member.client_id} entered"
end

channel.presence.on_leave do |member|
  puts "#{member.client_id} left"
end

channel.presence.get
channel.presence.on_members do |members|
  puts "Online: #{members.length}"
end
```

## History

```ruby
channel.history(limit: 50)
channel.on_history do |result|
  result.messages.each do |msg|
    puts "#{msg.publisher_id}: #{msg.data}"
  end
end
```

## Push Notifications

```ruby
push = WSocketIO::PushClient.new(
  base_url: 'https://node00.wsocket.online',
  token: 'secret',
  app_id: 'app1'
)

# Register FCM device
push.register_fcm(device_token: fcm_token, member_id: 'user-123')

# Send to a member
push.send_to_member('user-123', payload: {
  title: 'New message',
  body: 'You have a new message'
})

# Broadcast
push.broadcast(payload: { title: 'Announcement', body: 'Server update' })

# Channel targeting
push.add_channel('subscription-id', 'alerts')
push.remove_channel('subscription-id', 'alerts')

# VAPID key
vapid_key = push.get_vapid_key

# List subscriptions
subs = push.list_subscriptions('user-123')
```

## Requirements

- Ruby 3.0+

## License

MIT
