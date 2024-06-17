<?php

namespace Amp\Mysql\DBAL;

use Amp\Mysql\MysqlStatement as SqlStatement;
use Closure;
use Doctrine\DBAL\Driver\Exception;
use Doctrine\DBAL\Driver\Result;
use Doctrine\DBAL\Driver\Statement;
use Doctrine\DBAL\ParameterType;
use Throwable;
use function is_int;

class MysqlStatement implements Statement
{
    private SqlStatement $statement;

    private Closure $resultListener;

    private array $values = [];

    private array $types = [];

    public function __construct(SqlStatement $statement, callable $resultListener)
    {
        $this->statement = $statement;
        $this->resultListener = $resultListener instanceof Closure
            ? $resultListener
            : $resultListener(...);
    }

    public function bindValue($param, $value, ParameterType $type = ParameterType::STRING): void
    {
        $key = is_int($param) ? $param - 1 : $param;

        $this->values[$key] = $this->convertValue($value, $type);
    }

    public function execute($params = null): Result
    {
        $values = $this->values;

        if ($params !== null) {
            foreach ($params as $param) {
                $values[] = $param;
            }
        }

        // Convert references to correct types
        foreach ($this->types as $param => $type) {
            $values[$param] = $this->convertValue($values[$param], $type);
        }

        try {
            $result = $this->statement->execute($values);
            ($this->resultListener)($result);

            return new MysqlResult($result);
        } catch (Throwable $e) {
            throw MysqlException::new($e);
        }
    }

    private function convertValue($value, ParameterType $type): null|bool|int|string
    {
        return match ($type) {
            ParameterType::STRING, ParameterType::ASCII, ParameterType::LARGE_OBJECT, ParameterType::BINARY => (string) $value,
            ParameterType::INTEGER => (int) $value,
            ParameterType::BOOLEAN => (bool) $value,
            ParameterType::NULL => null,
        };
    }
}
