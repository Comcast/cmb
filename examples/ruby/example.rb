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

puts "Creating queue: #{queue_name}"
queue_url = sqs.create_queue(queue_name: queue_name)[:queue_url]
queue_arn = sqs.get_queue_attributes(queue_url: queue_url, attribute_names: ["QueueArn"])[:attributes]['QueueArn']

puts "Subscribing: #{queue_name} to #{topic_name}"
subscription_arn = sns.subscribe(topic_arn: topic_arn, endpoint: queue_arn, protocol: 'cqs')[:subscription_arn]

# publish a message on the topic

puts "Publishing to: #{topic_name}"
delivery = sns.publish(topic_arn: topic_arn, message: "test")[:message_id]

if delivery.nil?
  puts 'Published message was not delivered to any subscribers. '\
       'Check log file cmb.log.'
else
  puts 'Message was delivered to subscribers!'
end

