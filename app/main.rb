require 'pp'
require 'csv'
require 'kraken_ruby'


API_KEY = ''
API_SECRET = ''

# A stub for apps' entry point class
class Main

  attr_accessor :arg
  attr_accessor :kraken

  def initialize(arg)
    @arg = arg
    @kraken = Kraken::Client.new(API_KEY, API_SECRET)
  end

  def export_trades
    offset = 0
    total = 0
    CSV.open(File.join(File.dirname(__FILE__), '../data/orders.csv'), 'wb') do |csv|
      begin
        batch = kraken.closed_orders(:ofs => offset)
        total = batch['count'].to_i if total == 0
        orders = batch['closed']
        count = orders.size
        print "\nGot #{offset + 1}..#{offset + count} of #{total} orders, processing"

        if offset == 0
          col_info = orders.first.last.clone
          col_info['limitprice'] = ''
          descr = col_info.delete('descr')
          data = col_info.keys
          data.insert(0, 'id')
          data.concat(descr.keys)
          csv << data
        end

        offset += count
        #b = []
        orders.each do |order_id, order_info|
          order_info.delete('refcond')

          #a = order_info.keys
          #if a != b
          #  pp a
          #end
          #b = a

          order_descr = order_info.delete('descr')
          data = order_info.values
          data.insert(0, order_id)
          data.concat(order_descr.values)
          data.insert(14, '') unless order_info.has_key?('limitprice')
          csv << data.collect { |r| r != nil ? r : '' }
          print '.'
        end
      end while offset < total
    end
  end

  def export_ledger
    offset = 0
    total = 0
    CSV.open(File.join(File.dirname(__FILE__), '../data/ledger.csv'), 'wb') do |csv|
      begin
        batch = kraken.ledgers_info(:ofs => offset)
        total = batch['count'].to_i if total == 0
        orders = batch['ledger']
        count = orders.size
        print "\nGot #{offset + 1}..#{offset + count} of #{total} ledger records, processing"

        if offset == 0
          col_info = orders.first.last.clone
          data = col_info.keys
          data.insert(0, 'id')
          csv << data
        end

        offset += count
        orders.each do |order_id, order_info|
          order_descr = order_info
          data = order_info.values
          data.insert(0, order_id)
          csv << data.collect { |r| r != nil ? r : '' }
          print '.'
        end
        sleep 5
      rescue Net::ReadTimeout => ex
        print '\nNetwork timeout, retrying in 15 seconds'
        sleep 15
      rescue Exception => ex
        print '\nAPI rate exceeded, retrying in 15 seconds'
        sleep 15
      end while offset < total
    end
  end

  def run
    export_ledger
  end

end
