require 'semian'
require 'semian/adapter'
require 'pg'

module PG
  PG::Error.include(::Semian::AdapterError)

  class SemianError < PG::Error
    def initialize(semian_identifier, *args)
      super(*args)
      @semian_identifier = semian_identifier
    end
  end

  ResourceBusyError = Class.new(SemianError)
  CircuitOpenError = Class.new(SemianError)
end

module Semian
  module Postgresql
    include Semian::Adapter

    CONNECTION_ERROR = [
      PG::ConnectionBad,
      PG::UnableToSend,
      PG::QueryCanceled
    ]

    ResourceBusyError = ::PG::ResourceBusyError
    CircuitOpenError = ::PG::CircuitOpenError

    DEFAULT_HOST = 'localhost'
    DEFAULT_PORT = 3306

    QUERY_WHITELIST = Regexp.union(
      /\A\s*ROLLBACK/i,
      /\A\s*COMMIT/i,
      /\A\s*RELEASE\s+SAVEPOINT/i,
    )

    class SemianConfigurationChangedError < RuntimeError
      def initialize(msg = "Cannot re-initialize semian_configuration")
        super
      end
    end

    class << self
      attr_reader :semian_configuration

      def semian_configuration=(configuration)
        raise Semian::Postgresql::SemianConfigurationChangedError unless @semian_configuration.nil?
        @semian_configuration = configuration
      end

      def retrieve_semian_configuration(host, port)
        @semian_configuration.call(host, port) if @semian_configuration.respond_to?(:call)
      end
    end

    # The naked methods are exposed as `raw_query` and `raw_connect` for instrumentation purpose
    def self.included(base)
      base.send(:alias_method, :raw_async_exec, :async_exec)
      base.send(:remove_method, :async_exec)
    end

    def async_exec(*args)
      if query_whitelisted?(*args)
        raw_async_exec(*args)
      else
        acquire_semian_resource(adapter: :postgresql, scope: :async_exec) { raw_async_exec(*args) }
      end
    end

    def semian_identifier
      @semian_identifier ||= begin
        unless name = raw_semian_options && raw_semian_options[:name]
          host = conninfo_hash[:host] || DEFAULT_HOST
          port = conninfo_hash[:port] || DEFAULT_PORT
          name = "#{host}:#{port}"
        end
        :"postgresql_#{name}"
      end
    end

    private

    def query_whitelisted?(sql, *)
      QUERY_WHITELIST =~ sql
    rescue ArgumentError
      # The above regexp match can fail if the input SQL string contains binary
      # data that is not recognized as a valid encoding, in which case we just
      # return false.
      return false unless sql.valid_encoding?
      raise
    end

    def acquire_semian_resource(*)
      super
    rescue *CONNECTION_ERROR => error
      semian_resource.mark_failed(error)
      error.semian_identifier = self.semian_identifier
      raise
    end

    def raw_semian_options
      @raw_semian_options ||= Semian::Postgresql.retrieve_semian_configuration(
        conninfo_hash[:host],
        conninfo_hash[:port]
      )
    end
  end
end

::PG::Connection.include(Semian::Postgresql)
