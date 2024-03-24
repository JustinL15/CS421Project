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

        
        if (this.value == "Boolean") {
            switch (this.operation) {
                case "=":
                    return (this.left.value.equals(this.right.value));
                case "!=":
                    return (!this.left.value.equals(this.right.value));
                default:
                    throw new Exception("Operator " + this.operation + " not supported");
            }
        }
        int compareValue = comparison(record);
        switch (this.operation) {
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

    private Object resolve(Record record, LogicNode node) throws Exception {
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

    private int comparison(Record record) throws Exception {
        switch (this.value) {
            case "Integer":
                int li = (int) resolve(record, this.left);
                int ri = (int) resolve(record, this.right);
                if (li == ri) {
                    return 0;
                } else if (li < ri) {
                    return -1;
                } else {
                    return 1;
                }
            case "Double":
            double ld = (double) resolve(record, this.left);
            double rd = (double) resolve(record, this.right);
            if (ld == rd) {
                return 0;
            } else if (ld < rd) {
                return -1;
            } else {
                return 1;
            }
            case "String":
                String ls = (String) resolve(record, this.left);
                String rs = (String) resolve(record, this.right);
                return ls.compareTo(rs);
            default:
                throw new Exception("Type is not supported");
        }
    }
}
