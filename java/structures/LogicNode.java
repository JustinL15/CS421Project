import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Stack;
import java.util.List;

public class LogicNode {
    String value;
    String operation;
    LogicNode left;
    LogicNode right;

    public LogicNode(String operation) {
        this.operation = operation;
        this.value = null;
        this.left = null;
        this.right = null;
    }
    
    public LogicNode(String operation, String value) {
        this.operation = operation;
        this.value = value;
        this.left = null;
        this.right = null;
    }

    public boolean evaluate(Record record) throws Exception {
        if (this.operation.equals("And")) {
            return (this.left.evaluate(record) && this.right.evaluate(record));
        }
        if (this.operation.equals("Or")) {
            return (this.left.evaluate(record) || this.right.evaluate(record));
        }


        if (this.left.operation == "Boolean") {
            switch (this.value) {
                case "=":
                    return (this.left.value.equals(this.right.value));
                case "!=":
                    return (!this.left.value.equals(this.right.value));
                default:
                    throw new Exception("Operator " + this.operation + " not supported");
            }
        }
        int compareValue = comparison(record, this.left, this.right);
        switch (this.value) {
            case "<":
                return compareValue < 0;
            case ">":
                return compareValue > 0;
            case "=":
                return compareValue == 0;
            case ">=":
                return compareValue >= 0;
            case "<=":
                return compareValue <= 0;
            case "!=":
                return compareValue != 0;
            default:
                throw new Exception("Operator " + this.operation + " not supported");
        }
    }

    private static Object resolve(Record record, LogicNode node) throws Exception {
        switch (node.operation) {
            case "Attribute":
                List<Attribute> aList = record.getTemplate().getAttributes();
                for (int i = 0; i < aList.size(); i++) {
                    if (aList.get(i).getName().equals(node.value)) {
                        return record.getValues().get(i);
                    }
                }
                throw new Exception("Attribute " + node.value + " not found");
            case "Integer":
                return Integer.parseInt(node.value);
            case "Double":
                return Double.parseDouble(node.value);
            case "String":
                return node.value.substring(1, node.value.length() - 1);
            default:
                throw new Exception("Operation not supported: " + node.operation);
        }
    }

    private static int comparison(Record record, LogicNode leftNode, LogicNode rightNode) throws Exception {
        switch (leftNode.operation) {
            case "Integer":
                try {
                    int li = (int) resolve(record, leftNode);
                    int ri = (int) resolve(record, rightNode);
                    if (li == ri) {
                        return 0;
                    } else if (li < ri) {
                        return -1;
                    } else {
                        return 1;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new Exception("Type Mismatch");
                }
            case "Double":
                try {
                    double ld = (double) resolve(record, leftNode);
                    double rd = (double) resolve(record, rightNode);
                    if (ld == rd) {
                        return 0;
                    } else if (ld < rd) {
                        return -1;
                    } else {
                        return 1;
                    }
                } catch (Exception e) {
                    throw new Exception("Type Mismatch");
                }
            case "String":
                String ls = (String) resolve(record, leftNode);
                String rs = (String) resolve(record, rightNode);
                if (!isValidString(rs)) {
                    throw new Exception("Type Mismatch");
                } 
                return ls.compareTo(rs);
            case "Attribute":
                return comparison(record, convertType(leftNode, record), rightNode);
            default:
                throw new Exception("Type is not supported");
        }
    }

    public static LogicNode build(String conditions) throws Exception {
        Stack<LogicNode> stack = new Stack<>();
        for (int i = 0; i < conditions.length(); i++) {
            String type = "";
            StringBuilder tokenBuilder = new StringBuilder();
            while(i < conditions.length() && conditions.charAt(i) != ' ') {
                tokenBuilder.append(conditions.charAt(i));
                i++;
            }
            String token = tokenBuilder.toString();
            switch (token) {
                case "<":
                case ">":
                case "=":
                case ">=":
                case "<=":
                case "!=":
                    type = "Comparison";
                    if (stack.isEmpty()) {
                        throw new Exception("Invalid syntax for where condition.");
                    } else {
                        LogicNode lastNode = stack.pop();
                        if (lastNode.operation.equals("And") 
                        || lastNode.operation.equals("Or") 
                        || lastNode.operation.equals("Comparison")) {
                            throw new Exception("Invalid syntax for where condition.");
                        } else {
                            LogicNode newNode = new LogicNode(type, token);
                            newNode.left = lastNode;
                            stack.push(newNode);
                        }
                    }
                    break;
                case "And":
                    type = "And";
                    if (stack.isEmpty()) {
                        throw new Exception("Invalid syntax for where condition.");
                    } else {
                        LogicNode lastNode = stack.pop();
                        if (lastNode.operation.equals("Comparison") && lastNode.right != null) {
                            LogicNode newNode = new LogicNode(type);
                            newNode.left = lastNode;
                            stack.push(newNode);
                        } else {
                            throw new Exception("Invalid syntax for where condition.");
                        }
                    }
                    break;
                case "Or":
                    type = "Or";
                    if (stack.isEmpty()) {
                        throw new Exception("Invalid syntax for where condition.");
                    } else {
                        LogicNode lastNode = stack.pop();
                        if (lastNode.operation.equals("Comparison") && lastNode.right != null) {
                            if (stack.isEmpty()) {
                                LogicNode newNode = new LogicNode(type);
                                newNode.left = lastNode;
                                stack.push(newNode);
                            } else {
                                LogicNode nextNode = stack.pop();
                                while (nextNode.operation.equals("And")) {
                                    nextNode.right = lastNode;
                                    lastNode = nextNode;
                                    if (stack.isEmpty()) {
                                        nextNode = null;
                                    } else {
                                        nextNode = stack.pop();
                                    }
                                }
                                if (nextNode != null) {
                                    stack.push(nextNode);
                                }
                                LogicNode newNode = new LogicNode(type);
                                newNode.left = lastNode;
                                stack.push(newNode);
                            }
                        } else {
                            throw new Exception("Invalid syntax for where condition.");
                        }
                    }
                    break;
                default:
                    if (isValidBoolean(token)) {
                        type = "Boolean";
                    } else if (isValidIntegerInRange(token)) {
                        type = "Integer";
                    } else if (isValidDouble(token)) {
                        type = "Double";
                    } else if (isValidString(token)) {
                        type = "String";
                    } else {
                        type = "Attribute";
                    }
                    if (stack.isEmpty()) {
                        stack.push(new LogicNode(type, token));
                    } else {
                        LogicNode lastNode = stack.peek();
                        if (lastNode.operation.equals("Comparison")) {
                            lastNode.right = new LogicNode(type, token);
                        }
                        else if (lastNode.operation.equals("And") || lastNode.operation.equals("Or")) {
                            stack.push(new LogicNode(type, token));
                        } else {
                            throw new Exception("Invalid syntax for where condition.");
                        }
                    }
                    break;
            }
        }
        LogicNode nextNode = stack.pop();
        if (!nextNode.operation.equals("Comparison")) {
            throw new Exception("Invalid syntax for where condition.");
        } else {
            LogicNode lastNode = nextNode;
            while (!stack.isEmpty()) {
                nextNode = stack.pop();
                nextNode.right = lastNode;
                lastNode = nextNode;
            }
            return lastNode;
        }
    }

    public static boolean isValidBoolean(String str) {
        return str.equals("true") || str.equals("false"); 
    }
    public static boolean isValidString(String str) {
        if (str.length() < 2) {
            return false;
        }
        if (str.charAt(0) != '"') {
            return false;
        }
        int qoutecount = 1;
        for (int i = 1; i < str.length(); i++) {
            if (str.charAt(i) == '"') {
                qoutecount += 1;
            }
        }
        if (qoutecount == 2) {
            return true;
        }
        return false;
    }

    public static boolean isValidIntegerInRange(String str) {
        try {
            int value = Integer.parseInt(str);
            return value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public static boolean isValidDouble(String str) {
        try {
            Double.parseDouble(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private static LogicNode convertType(LogicNode node, Record record) throws Exception {
        List<Attribute> aList = record.getTemplate().getAttributes();
        for (int i = 0; i < aList.size(); i++) {
            if (aList.get(i).getName().equals(node.value)) {
                String typename = aList.get(i).getDataType().name();
                if (typename.equals("Char") || typename.equals("Varchar")) {
                    typename = "String";
                }
                return new LogicNode(typename, record.getValues().get(i).toString());
            }
        }
        throw new Exception("Attribute " + node.value + " not found");
    }
}
