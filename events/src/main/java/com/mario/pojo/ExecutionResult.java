package com.mario.pojo;

public class ExecutionResult<T> {
    private final T data;
    private final Error error;
    private final boolean success;

    public ExecutionResult(T data, Error error, boolean success) {
        this.data = data;
        this.error = error;
        this.success = success;
    }

    public static <T> ExecutionResult<T> success(T data) {
        return new ExecutionResult<>(data, null, true);
    }

    public static <T> ExecutionResult<T> error(Error error) {
        return new ExecutionResult<>(null, error, false);
    }

    public T getData() {
        return data;
    }

    public Error getError() {
        return error;
    }

    public String getErrorMessage() {
        return error.toString();
    }

    public boolean isSuccess() {
        return success;
    }

    @Override
    public String toString() {
        return "ExecutionResult{" +
                "data=" + data +
                ", error=" + error +
                ", sucess=" + success +
                '}';
    }

}
