package pt.bayonnesensei.export_sales.service.dto;

public record ObjectDTO(String name, String contentType, Long size, byte [] data) {
}
