#!/usr/bin/ruby

require 'optparse'

# Monkey patch String for numeric strings detection
class String
  def is_number?
    true if Float(self) rescue false
  end
end

def combine(params, options, &block) 
  if options.count == 0
    yield params
  else
    for option in options[0]
     combine params.dup.concat([option]), options[1..options.count-1], &block
    end
  end
end


options = ARGV[1..ARGV.count-1].map do |arg|
  if arg[0] == '['
    eval arg 
  else  
    [arg]
  end
end

File.open('benchmark_'+options.map{|o|o.join(',').gsub('/', '')}.join('_')+'.csv', 'w') do |f|
  combine [], options do |params|
    print "Running #{params.join(',')}\n"
    f.write params.join(',')
    out = `/usr/bin/time -f "%e" #{ARGV[0]} #{params.join(' ')} 2>&1`
    time = out.split("\n").last
    f.write ",#{time}\n"
  end
end

 
