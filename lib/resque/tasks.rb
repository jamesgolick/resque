# require 'resque/tasks'
# will give you the resque tasks

namespace :resque do
  task :setup

  desc "Start a Resque worker"
  task :work => :setup do
    require 'resque'

    worker = nil
    queues = (ENV['QUEUES'] || ENV['QUEUE']).to_s.split(',')

    begin
      worker = Resque::Worker.new(*queues)
      worker.verbose = ENV['LOGGING'] || ENV['VERBOSE']
      worker.very_verbose = ENV['VVERBOSE']
    rescue Resque::NoQueueError
      abort "set QUEUE env var, e.g. $ QUEUE=critical,high rake resque:work"
    end

    puts "*** Starting worker #{worker}"

    worker.work(ENV['INTERVAL'] || 5) # interval, will block
  end

  desc "Start multiple Resque workers. Should only be used in dev mode."
  task :workers do
    threads = []

    ENV['COUNT'].to_i.times do
      threads << Thread.new do
        system "rake resque:work"
      end
    end

    threads.each { |thread| thread.join }
  end

  desc "Start the preforking Resque worker."
  task :preforking_worker do
    require 'resque'

    GC.respond_to?(:copy_on_write_friendly=) && GC.copy_on_write_friendly = true

    queues       = (ENV['QUEUES'] || ENV['QUEUE']).to_s.split(',')
    verbose      = ENV['LOGGING'] || ENV['VERBOSE']
    very_verbose = ENV['VVERBOSE']
    workers      = ENV['WORKERS'].to_i
    interval     = ENV['INTERVAL'] || 5

    master = Resque::Master.new(workers, queues, verbose,
                                very_verbose, interval)
    master.start
  end
end
