require "fileutils"
require "zlib"

class Bitcask
  MAX_FILE_SIZE = 100_000_000 # 100 MB
  LOCKFILE_NAME = "bitcask.lock"

  class BitcaskLockedError < StandardError; end

  # Open a new or existing Bitcask datastore with additional options. Valid
  # options include read write (if this process is going to be a writer and not
  # just a reader) and sync on put (if this writer would prefer to sync the
  # write file after every write operation).
  # The directory must be readable and writable by this process, and only one
  # process may open a Bitcask with read write at a time.
  #
  # Open a new or existing Bitcask datastore for read-only access. The
  # directory and all files in it must be readable by this process.
  def initialize directory_name, opts = {}
    @cask_directory = directory_name
    FileUtils.mkdir_p @cask_directory

    @lockfile = "#{@cask_directory}/#{LOCKFILE_NAME}"
    if File.exist? @lockfile
      raise BitcaskLockedError.new "lockfile is present, other instance running?"
    else
      File.write @lockfile, ""
    end

    # populate keydict from hint file or data files
    @keydict = read_data_and_hint_files

    # Keep file handles for all old data files open
    #   path => file handle
    @old_data_files = {}

    # open new data file to write to
    @data_file = open_new_data_file
  end

  # Retrieve a value by key from a Bitcask datastore.
  def get key
    entry = @keydict[key]

    if entry
      if entry[0] == @data_file.path
        pos = @data_file.pos
        @data_file.pos = entry[2]
        value = @data_file.read entry[1]
        @data_file.pos = pos
        value
      else
        data_file = @old_data_files[entry[0]]
        unless data_file
          data_file = File.open(entry[0], "rb")
          @old_data_files[entry[0]] = data_file
        end

        data_file.pos = entry[2]
        data_file.read entry[1]
      end
    end
  end

  # Store a key and value in a Bitcask datastore.
  def put key, value
    # append value to open file
    # write new keydict value
    @keydict[key] = write_data_entry key, value

    # sync

    if @data_file.size > MAX_FILE_SIZE
      fname = @data_file.path
      @data_file.close
      @data_file = open_new_data_file
      @old_data_files[fname] = File.open(fname, "rb")
    end
  end

  # Delete a key from a Bitcask datastore.
  def delete key
    write_data_entry key, nil, true
    @keydict[key] = nil

    sync
  end

  # List all keys in a Bitcask datastore.
  def list_keys
    @keydict.keys
  end

  # Fold over all K/V pairs in a Bitcask datastore.
  # Fun is expected to be of the form: F(K,V,Acc0) → Acc.
  # def fold fun, acc
  # end

  # Merge several data files within a Bitcask datastore into a more compact
  # form. Also, produce hintfiles for faster startup.
  def merge
    deletable_data_files = Dir["#{@cask_directory}/**.data"]
    deletable_data_files.delete @data_file.path

    # write values for keys from old data files into the active one
    @keydict.each do |key, dict|
      data_file = dict.first
      next if data_file == @data_file.path # skip active data file

      put key, get(key)

      # deletable_data_files.add(data_file)
    end

    File.delete(*deletable_data_files)
    deletable_hint_files = deletable_data_files
      .map { |df| "#{df}.hint" }
      .select { |hf| File.exist? hf }
    File.delete(*deletable_hint_files)

    write_hint_files

    true
  end

  # Force any writes to sync to disk.
  def sync
    @data_file.fsync
  end

  # Close a Bitcask data store and flush all pending writes (if any) to disk.
  def close
    sync

    @data_file.close

    write_hint_files

    File.delete @lockfile
  end

  private

  def read_data_and_hint_files
    dict = {}
    # TODO: read hint file

    hint_files = Dir["#{@cask_directory}/**.hint"].sort
    data_files = Dir["#{@cask_directory}/**.data"].sort

    hint_files.each do |hint_file|
      file = File.read(hint_file).b
      fname = hint_file.sub(".hint", "")

      size = file.size
      offset = 0
      while offset < size
        key, dict_entry, offset = read_hint_entry file, offset, fname
        dict[key] = dict_entry
      end

      data_files.delete hint_file.sub ".hint", ""
    end

    data_files.each do |data_file|
      file = File.open(data_file, "rb")

      until file.eof?
        key, dict_entry = read_data_entry file

        if dict_entry.nil?
          dict.delete(key)
        else
          dict[key] = dict_entry
        end
      end
    end

    dict
  end

  def read_hint_entry file, offset, fname
    # ts  ksz vsz vpos
    meta_length = 14 # 4 + 2 + 4 + 4
    meta = file[offset..offset + meta_length - 1]

    offset += meta_length

    tstamp, key_sz, value_sz, value_pos = meta.unpack "NnNN"
    key = file[offset..offset + key_sz - 1]

    offset += key_sz

    [key, [fname, value_sz, value_pos, tstamp], offset]
  end

  def read_data_entry file
    # crc ts del  ksz  vsz
    meta_length = 15 # 4 + 4 + 1 + 2 + 4
    meta = file.read(meta_length)

    _crc, tstamp, delete, key_sz, value_sz = meta.unpack("NNCnN")

    # TODO: check CRC

    key = file.read(key_sz)
    value_pos = file.pos
    file.pos = file.pos + value_sz

    return [key, nil] if delete == 1

    # | file_id | value_sz | value_pos | tstamp |
    [key, [file.path, value_sz, value_pos, tstamp]]
  end

  def write_data_entry key, value, delete = false
    tstamp = Time.now.utc.to_i
    key_sz = key.bytesize
    value_sz = value.nil? ? 0 : value.bytesize
    value_pos = @data_file.pos + 4 + 4 + 1 + 2 + 4 + key_sz
    delete = delete ? 1 : 0

    # data file
    # | CRC | tstamp | tombstone | ksz | value_sz | key | value |
    # CRC ( |---------------------------------------------------| )
    #   32 bit int
    # tstamp
    #   32 bit int
    # tombstone
    #   8 bit unsigned int
    # ksz
    #   16 bit int
    # value_sz
    #   32 bit int
    meta = [tstamp, delete, key_sz, value_sz].pack "NCnN"

    payload = "#{meta}#{key}#{value}"
    crc = [Zlib.crc32(payload)].pack("N")

    @data_file.write "#{crc}#{payload}"

    # keydict
    # | file_id | value_sz | value_pos | tstamp |
    [@data_file.path, value_sz, value_pos, tstamp]
  end

  def open_new_data_file
    File.new "#{@cask_directory}/#{Time.now.to_i}.data", "a+b"
  end

  def write_hint_files
    previous_data_file = nil
    hint_file = nil

    @keydict.each do |key, dict|
      data_file, value_sz, value_pos, tstamp = *dict
      unless data_file == previous_data_file
        hint_file&.close
        previous_data_file = data_file
        hint_file = File.open "#{data_file}.hint", "w+b"
      end
      meta = [tstamp, key.bytesize, value_sz, value_pos].pack "NnNN"
      hint_file.write "#{meta}#{key}"
    end
  end
end
