package cn.edu.whu.lynn.exception;

/**
 * @author Lynn Lee
 * @date 2023/12/29
 **/


import java.io.PrintWriter;
import java.io.StringWriter;

public class ButterflyException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private ErrorCode errorCode;

    public ButterflyException(ErrorCode errorCode, String errorMessage) {
        super(errorCode.toString() + " - " + errorMessage);
        this.errorCode = errorCode;
    }

    public ButterflyException(String errorMessage) {
        super(errorMessage);
    }

    private ButterflyException(ErrorCode errorCode, String errorMessage, Throwable cause) {
        super(errorCode.toString() + " - " + getMessage(errorMessage) + " - " + getMessage(cause), cause);

        this.errorCode = errorCode;
    }

    public static ButterflyException asDataXException(ErrorCode errorCode, String message) {
        return new ButterflyException(errorCode, message);
    }

    public static ButterflyException asDataXException(String message) {
        return new ButterflyException(message);
    }

    public static ButterflyException asDataXException(ErrorCode errorCode, String message, Throwable cause) {
        if (cause instanceof ButterflyException) {
            return (ButterflyException) cause;
        }
        return new ButterflyException(errorCode, message, cause);
    }

    public static ButterflyException asDataXException(ErrorCode errorCode, Throwable cause) {
        if (cause instanceof ButterflyException) {
            return (ButterflyException) cause;
        }
        return new ButterflyException(errorCode, getMessage(cause), cause);
    }

    public ErrorCode getErrorCode() {
        return this.errorCode;
    }

    private static String getMessage(Object obj) {
        if (obj == null) {
            return "";
        }

        if (obj instanceof Throwable) {
            StringWriter str = new StringWriter();
            PrintWriter pw = new PrintWriter(str);
            ((Throwable) obj).printStackTrace(pw);
            return str.toString();
            // return ((Throwable) obj).getMessage();
        } else {
            return obj.toString();
        }
    }
}