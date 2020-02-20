# typed: false
require "./bitcask"

print " "
puts `du -sh some_cask/*`.strip

puts "Open Bitcask #{Time.now}"
bitcask = Bitcask.new "some_cask"
puts "Bitcask opened #{Time.now}"
puts

keys = bitcask.list_keys
puts "Start get (#{keys.size} keys) #{Time.now}"
while (key = keys.pop)
  bitcask.get(key)
end
puts "End get #{Time.now}"
puts

if File.exist?('/usr/share/dict/words')
  puts "Write #{`wc -l /usr/share/dict/words`.strip} keys"

  puts "Start Write #{Time.now}"

  wordlist = File.open "/usr/share/dict/words"
  wordlist.each do |word|
    word = word.strip
    bitcask.put word, "hi #{word}"
  end

  puts "End Write #{Time.now}"
  puts

  bitcask.sync
end

puts "Write #{`wc -l words.txt`.strip} keys"

puts "Start Write #{Time.now}"

wordlist = File.open "words.txt"
wordlist.each do |word|
  word = word.strip
  bitcask.put word, "hi #{word}"
end

puts "End Write #{Time.now}"
puts

bitcask.sync

keys = bitcask.list_keys
puts "Start get (#{keys.size} keys) #{Time.now}"
while (key = keys.pop)
  bitcask.get key
end
puts "End get #{Time.now}"
puts

puts "Start merge #{Time.now}"
bitcask.merge
puts "End merge #{Time.now}"

bitcask.close

# bitcask.put "Hello", "Howdy World how's it going?"
# val = bitcask.get "Hello"
# puts val.inspect

# bitcask.merge
# bitcask.delete "Hello"

# val = bitcask.get "Hello"
# puts val.inspect

# bitcask.put "Hi", ""
# puts bitcask.get("Hi").inspect

# puts bitcask.get("I don't exist").inspect
