module com.example.alr3attemptforassignment {
    requires javafx.controls;
    requires javafx.fxml;
    requires java.sql.rowset;


    opens com.example.alr3attemptforassignment to javafx.fxml;
    exports com.example.alr3attemptforassignment;
}