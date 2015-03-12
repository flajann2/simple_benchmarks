# -*- coding: utf-8 -*-
module Simple

  # Metrics keeper and reporter mixin
  module Metrics
    # timeout of the redis queue in seconds
    QUEUE_TIMEOUT = 1800

    class << self
      def included(klazz)
        @@usedin = [] if defined?(@@usedin).nil?
        @@usedin << klazz
        @@counters = {} if defined?(@@counters).nil?
        @@tally = {} if defined?(@@tally).nil?
        @@start_time = DateTime.now if defined?(@@start_time).nil?
        @@bulk_times = []
        @@general_timer = {} 
      end
    end

    # Count one on the basis of the tag.
    # if the datum is given, we simply pass it through.
    def tick(datum = nil, tag: nil)
      tag = caller_locations(1,1).first.inspect if tag.nil?
      @@counters[tag] ||= 0
      @@counters[tag] += 1
      datum
    end
    
    # Measure the time a job takes.
    # This will allow us to get pretty fine-grained.
    def measure(tag, &block)
      @@general_timer[tag] ||= []
      before = Time.now
      ret = block.()
      after = Time.now
      @@general_timer[tag] << [before, after - before]
      ret
    end
      
    # Aspect-invoked when bulk#execute is called.
    # The Time objects are expected here.
    def bulk_execute_tick(before, after)
      @@bulk_times << [before, after - before]
    end
    
    
    # Tally up object totals.
    def tally(ob: nil, count: 1)
      @@tally[ob] ||= 0
      @@tally[ob] += count
    end
    
    def get_counters;       @@counters; end
    def get_usedin;         @@usedin; end
    def get_tally;          @@tally; end
    def get_start_time;     @@start_time; end
    def get_bulk_times;     @@bulk_times; end
    def get_general_timer;  @@general_timer; end
    
    def init_report_database
      @@redis = Redis.new(:driver => :hiredis)
    end

    def update_report_database
      @@redis.lpush :metrics, get_general_timer.to_json
    end

    def get_general_timer_from_database(forks, tout, skip)
      forks.times.map{ |i|
        json = @@redis.brpop(:metrics, timeout: tout)
        json = json.last unless json.nil?
        unless json.nil?
          JSON.parse json
        else
          mess = "Timed out waiting for queue (#{tout} sec, fork #{i})."
          if skip
            puts mess
            nil
          else
            raise mess
          end
        end
      }.compact.inject({}){ |memo, h|
        h.each{|key, ar|
          memo[key] ||= []
          memo[key] += ar
        }
        memo
      }
    end

    def render_report(forks, timeout = QUEUE_TIMEOUT, skip = false)
      gr = nil
      metrics = [
                 'Report Metrics',
                 gr = {general_report:
                   get_general_timer_from_database(forks, timeout, skip).inject({}){ |mm, (tag, timings)|
                     t = timings.reduce(Hash.new(0)){ |memo, (stamp, duration) |
                       memo[:opsec] = Hash.new(0) unless memo[:opsec].kind_of? Hash 
                       memo[:opsec][DateTime.parse(stamp).to_time.to_i] += 1
                       memo[:total] += duration
                       memo[:highest] = duration unless memo[:highest] > duration
                       memo[:lowest] = 1.0/0.0 if memo[:lowest].kind_of? Fixnum 
                       memo[:lowest] = (duration * 1.0) unless memo[:lowest] < duration
                       memo
                     }
                     t[:opsec]    = t[:opsec].values.inject(:+) * 1.0 / t[:opsec].size
                     t[:tag]      = tag
                     t[:count]    = timings.size * 1.0
                     t[:average]  = t[:total] / t[:count]
                     t[:variance] = timings.reduce(0.0){ | memo, (ignore, duration) |
                       e = duration - t[:average]
                       memo += e*e
                     } / t[:count]
                     t[:sd] = Math.sqrt t[:variance]
                     mm[tag] = t
                     mm
                   }.map{ |tag, t|
                     sprintf "%15<tag>s |  %9<count>d |%7.3<average>f |%7.3<highest>f |%7.3<lowest>f |%7.3<sd>f | %9.3<total>f | %7.1<opsec>f " % t
                   }.unshift("     TAGS       |   COUNT    |  AVE   |HIGHEST | LOWEST |   SD   | TOTDUR    | ops/sec")
                     .join("\n")
                 },
                 "Running time: %5.3f minutes" % [(DateTime.now - get_start_time).to_f * 24 * 60],
                 gr[:general_report]
                ]
      puts( unless env.verbose
              metrics[-2..-1]
            else
              metrics
            end.join("\n" + '#' * 87 + "\n"))
    end
  end
end

