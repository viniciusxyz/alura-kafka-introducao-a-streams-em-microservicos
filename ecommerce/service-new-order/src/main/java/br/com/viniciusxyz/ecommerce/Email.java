package br.com.viniciusxyz.ecommerce;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class Email {
    private final String subject,  body;
}
