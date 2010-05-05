module Resque
  class Master
    attr_reader :verbose, :very_verbose

    def initialize(num_workers, queues, verbose, very_verbose, interval)
      @num_workers  = num_workers
      @queues       = queues
      @verbose      = verbose
      @very_verbose = very_verbose
      @interval     = interval
      @workers      = {}
    end

    def start
      procline "(startup)"
      register_signal_handlers
      loop do
        reap_workers
        start_missing_workers
        procline "(running)"
        sleep(2)
      end
    end

    def start_missing_workers
      missing_workers.times do
        worker              = Worker.new(*@queues)
        worker.verbose      = @verbose
        worker.very_verbose = @very_verbose
        pid                 = fork_worker(worker)
        @workers[pid]       = worker
        log "Started worker at #{pid}."
      end
    end

    def fork_worker(worker)
      fork do
        if Resque.after_prefork
          log! "Executing after_prefork hook."
          Resque.after_prefork.call
        end

        worker.work(@interval)
      end
    end

    def missing_workers
      @num_workers - @workers.size
    end
    
    def reap_workers
      begin
        loop do
          wpid, status = Process.waitpid2(-1, Process::WNOHANG)
          break if !wpid
          log! "Reaped #{wpid}"
          @workers.delete(wpid)
        end
      rescue Errno::ECHILD
      end
    end

    def register_signal_handlers
      trap('TERM') { shutdown }
      trap('INT')  { shutdown }
      trap('QUIT') { shutdown }

      log! "Registered signals"
    end

    def shutdown(timeout = 10)
      log "Shutting down."
      procline "(shutting down)"
      limit = Time.now + timeout
      until @workers.empty? || Time.now > limit
        kill_all_workers
        sleep(0.1)
        reap_workers
      end
      kill_all_workers("KILL")
      exit
    end

    def kill_all_workers(signal = "QUIT")
      @workers.keys.each { |pid| Process.kill(signal, pid) }
    end

    def procline(string)
      $0 = "resque-master-#{Resque::Version}: #{string}"
    end

    # Log a message to STDOUT if we are verbose or very_verbose.
    def log(message)
      if verbose
        puts "*** #{message}"
      elsif very_verbose
        time = Time.now.strftime('%I:%M:%S %Y-%m-%d')
        puts "** [#{time}] #$$: #{message}"
      end
    end

    # Logs a very verbose message to STDOUT.
    def log!(message)
      log message if very_verbose
    end
  end
end
