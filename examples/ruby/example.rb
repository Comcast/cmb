#
# Usage:
#
# bundle install
# ACCESS_KEY="cmb user's access key" SECRET_KEY="cmb user's secret key" ruby ./example.rb
#
#

require 'rubygems'
require 'bundler/setup'

Bundler.require :default

access_key = ENV['ACCESS_KEY']
secret_key = ENV['SECRET_KEY']

sns_endpoint = "http://localhost:6061/"
sqs_endpoint = "http://localhost:6059/"

Aws.config[:credentials] = Aws::Credentials.new access_key, secret_key
Aws.config[:region] = 'csv'

sns = Aws::SNS::Client.new endpoint: sns_endpoint
sqs = Aws::SQS::Client.new endpoint: sqs_endpoint

# create unique topic and queue names via the current time

topic_name = "topic-#{Time.now.to_i}"
queue_name = "queue-#{Time.now.to_i}"

# remove any existing topics and queues in this account

sns.list_topics[:topics].map { |topic| topic[:topic_arn] }.compact.each do |arn|
  puts "Deleting topic: #{arn}"
  sns.delete_topic topic_arn: arn
end

sqs.list_queues[:queue_urls].each do |url|
  puts "Deleting queue: #{url}"
  sqs.delete_queue queue_url: url
end

# create the topic, queue and subscription

puts "Creating topic: #{topic_name}"
topic_arn = sns.create_topic(name: topic_name)[:topic_arn]
topic = Aws::SNS::Topic.new topic_arn, client: sns

puts "Creating queue: #{queue_name}"
queue_url = sqs.create_queue(queue_name: queue_name)[:queue_url]
queue = Aws::SQS::Queue.new queue_url, client: sqs

puts "Subscribing: #{queue_name} to #{topic_name}"
subscription = topic.subscribe(endpoint: queue.arn, protocol: 'cqs')
subscription.set_attributes attribute_name: 'RawMessageDelivery', attribute_value: 'true'

# publish a message on the topic

puts "Publishing to: #{topic_name}"

delivery = topic.publish message: 'test', message_attributes: {
  'ruby_example.key_1' => {
    string_value: 'xyz',
    data_type: 'String'
  },

  'ruby_example.key_2' => {
    string_value: '123',
    data_type: 'Number'
  }
}

if delivery.nil?
  puts 'Published message was not delivered to any subscribers!'
  exit 1
end

sleep(1)

puts "Receiving messages from: #{queue_name}"
resp = queue.receive_messages(:message_attribute_names => [ "All" ])
messages = resp.to_a

if messages.empty?
  puts "No message was received!"
  exit 1
end

message = messages.first

puts "Message..."

ap message_id: message.message_id,
   receipt_handle: message.receipt_handle,
   attributes: message.attributes,
   body: message.body,
   message_attributes: message.message_attributes
