#!/usr/bin/env ruby
require_relative '../lib/environment'

#raise 'Usage: repo=octokit.py bin/app.rb' unless ENV['repo']

#Main.new(ENV['repo']).run

# 1. Iterate over data/exchanges directory and read all JSON files
# 2. Iterate over ledger-*.csv files and read all data from them
# 3. Generate daily timestamps
# 4. start with values from previous day
# 5. iterate over ledgers until new day timestamp met, update values
# 6. save results to db
# data structure: timestamp(daily), (cry_orig, cry_eur), total_eur, fee_eur, tradevol_eur

require 'csv'
require 'json'

puts 'Reading exchange rates from files'
exch_rates = {}
exch_files = Dir["#{File.dirname(__FILE__)}/../data/exchanges/*.json"]
exch_files.each do |exch_file|
  curr = exch_file.split('/').last.split('eur').first.upcase
  print "Reading #{curr} to EUR ..."
  blob = File.read(exch_file)
  exch_data = JSON.parse(blob)
  exch_rates[curr] = exch_data['Data']
  puts " #{exch_rates[curr].size} days read"
rescue Exception => ex
  puts "Can't read file #{exch_file}, exception"
  pp ex
end

puts 'Reading ledgers from files'
ledgers = []
ledger_files = Dir["#{File.dirname(__FILE__)}/../data/@ledger-*.csv"]
ledger_files.each do |ledger_file|
  print "Reading #{ledger_file.split('/').last} ..."
  ledger = CSV.read(ledger_file)
  ledger.shift
  ledger.reverse!
  ledgers.push(ledger)
  puts " #{ledger.size} entries"
rescue Exception => ex
  puts "Can't read ledger #{exch_file}, exception"
  pp ex
end
lidx = {
  :timestamp => 2,
  :type => 3,
  :exchange => 4,
  :curr => 5,
  :amount => 6,
  :fee => 7,
  :balance => 8
}

currencies = exch_rates.keys
portfolio = []
wallet = { 'EUR' => 0 }
basecurr = exch_rates.keys.first
prev = { :timestamp => 0, :total => 0, :fee => 0, :vol => 0, 'EUR' => 0 }
currencies.each { |c| prev[c] = 0; prev["#{c}_EUR"] = 0 }
currencies.each { |c| wallet[c] = 0 }
day = 0
cursors = ledgers.map { 0 }
exch_rates[basecurr].each do |rec|
  dayts = rec['time']
  prevdayts = dayts - 24 * 60 * 60
  print "Processing day #{dayts} "
  curr = prev.clone
  curr[:fee] = 0
  curr[:total] = 0
  curr[:vol] = 0
  curr[:timestamp] = dayts
  ledgers.each_with_index do |ledger, ledger_no|
    print '('
    cursor = cursors[ledger_no]
    while cursor < ledger.count and ledger[cursor][lidx[:timestamp]].to_i < prevdayts
      print '-'
      cursor += 1
    end
    while cursor < ledger.count and ledger[cursor][lidx[:timestamp]].to_i < dayts
      print '.'
      lrec = ledger[cursor].clone
      cursor += 1
      c = lrec[lidx[:curr]]
      c[0] = '' if (c[0] == 'X' and c.size == 4) or (c[0] == 'Z' and c != 'ZEC')
      c = 'BTC' if c == 'XBT'
      c = 'USD' if c == 'USDT'
      if exch_rates[c] == nil and c != 'EUR'
        puts "NO EXCHANGE RATE FOR #{c} currency!!!"
        next
      end
      ex = c == 'EUR' ? 1 : exch_rates[c][day]['close']

      if lrec[lidx[:type]] == 'withdrawal'
        wallet[c] += lrec[lidx[:amount]].to_f.abs
      elsif lrec[lidx[:type]] == 'deposit' and c != 'EUR'
        diff = lrec[lidx[:amount]].to_f.abs
        wallet[c] -= diff if diff <= wallet[c]
        curr[c] -= diff if lrec[lidx[:exchange]] == 'poloniex'
      end

      if lrec[lidx[:exchange]] == 'poloniex'
        curr[c] += lrec[lidx[:amount]].to_f
      else
        curr[c] = lrec[lidx[:balance]].to_f + wallet[c]
      end
      curr[:fee] += lrec[lidx[:fee]].to_f * ex
      curr[:vol] += lrec[lidx[:amount]].to_f.abs * ex
    end
    cursors[ledger_no] = cursor
    print ') '
  end
  print "\n"
  currencies.each { |c| curr["#{c}_EUR"] = curr[c] * exch_rates[c][day]['close'] if c != 'EUR' }
  curr[:total] = currencies.sum { |c| curr["#{c}_EUR"] }
  portfolio.push(curr)
  prev = curr.clone
  day += 1
end

CSV.open(File.join(File.dirname(__FILE__), '../data/portfolio.csv'), 'wb') do |csv|
  csv << portfolio.first.keys
  portfolio.each { |p| csv << p.values }
end
