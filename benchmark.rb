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

# Main: Parse arguments
n = 1

if ARGV.count == 0
  print "Usage: benchmark.rb [-n COUNT] FILE ARG1 ARG2 ARG...\n"
  exit 1
end

if ARGV[0] == '-n'
  n = Integer(ARGV[1])
  ARGV = ARGV[2..ARGV.count-1]
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
    
    for i in 1..n do 
      f.write params.join(',')
      out = `#{ARGV[0]} #{params.join(' ')}`
      time = out.split("\n").last
      f.write ",#{time}\n"
    end
  end
end

 
