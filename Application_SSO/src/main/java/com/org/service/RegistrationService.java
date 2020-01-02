package com.org.service;

import java.util.ArrayList;
import java.util.List;
import javax.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PostAuthorize;
import org.springframework.security.access.prepost.PostFilter;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Service;
import org.springframework.validation.annotation.Validated;
import com.org.dto.RegistrationDto;
import com.org.entity.RegistrationEntity;
import com.org.repository.RegistrationRepository;
import com.org.validation.ValidAuthorize;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Service
@Validated
public class RegistrationService {

	@Autowired
	private RegistrationRepository registrationRepository;

	@ValidAuthorize
	public Long saveRegistrationDetails(@Valid RegistrationDto registrationDto) {

		RegistrationEntity registrationEntity = new RegistrationEntity();
		registrationEntity.setFirstName(registrationDto.getFirstName());
		registrationEntity.setLastName(registrationDto.getLastName());
		registrationEntity.setEmail(registrationDto.getEmail());
		registrationEntity.setPassword(registrationDto.getPassword());

		RegistrationEntity entity = registrationRepository.save(registrationEntity);
		
		if(entity.getId() == null) {
			log.warn("Unable to Register User in SQL");
		}
		
		log.info("Successfully Register User in SQL");
		
		return entity.getId();
	}

	@PreAuthorize("#email == authentication.name")
	@PostAuthorize("returnObject.email == authentication.principal.username")
	public RegistrationEntity findByEmail(String email) {

		RegistrationEntity registrationEntity = registrationRepository.findByEmail(email);

		if (registrationEntity != null) {
			return registrationEntity;
		}

		return null;
	}

	@ValidAuthorize
	@PostFilter(value = "filterObject != authentication.principal.username")
	public List<RegistrationDto> findAll() {

		List<RegistrationDto> list = new ArrayList<RegistrationDto>();
		Iterable<RegistrationEntity> registrationEntities = registrationRepository.findAll();

		for (RegistrationEntity registrationEntity : registrationEntities) {

			RegistrationDto registrationDto = new RegistrationDto();
			registrationDto.setFirstName(registrationEntity.getFirstName());
			registrationDto.setLastName(registrationEntity.getLastName());
			registrationDto.setEmail(registrationEntity.getEmail());
			registrationDto.setPassword(registrationEntity.getPassword());
			list.add(registrationDto);
		}

		return list;
	}
}
