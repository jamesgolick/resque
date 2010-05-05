module Resque
  class Master
    def initialize(num_workers, queues, verbose, very_verbose, interval)
      @num_workers  = num_workers
      @queues       = queues
      @verbose      = verbose
      @very_verbose = very_verbose
      @interval     = interval
      @workers      = {}
    end

    def start
      procline "(master)"
      loop do
        reap_workers
        start_missing_workers
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
      end
    end

    def fork_worker(worker)
      fork { worker.work(@interval) }
    end

    def missing_workers
      @num_workers - @workers.size
    end
    
    def reap_workers
      begin
        loop do
          wpid, status = Process.waitpid2(-1, Process::WNOHANG)
          break if !wpid
          puts "Reaped #{wpid}"
          @workers.delete(wpid)
        end
      rescue Errno::ECHILD
      end
    end

    def procline(string)
      $0 = "resque-#{Resque::Version}: #{string}"
    end
  end
end
