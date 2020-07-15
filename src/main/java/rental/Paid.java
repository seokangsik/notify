
package rental;

public class Paid extends AbstractEvent {

    private Long id;
    private Long orderId;
    private String approvalDate;
    private Integer rentalPrice;
    private String status;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }
    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }
    public String getPaidDate() {
        return approvalDate;
    }

    public void setPaidDate(String approvalDate) {
        this.approvalDate = approvalDate;
    }
    public Integer getRentalPrice() {
        return rentalPrice;
    }

    public void setRentalPrice(Integer rentalPrice) {
        this.rentalPrice = rentalPrice;
    }
    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
