package com.org.entity;

import java.util.Collection;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.Table;
import lombok.Data;

@Data
@Entity
@Table(name = "TBL_REGISTRATION")
public class RegistrationEntity {

	@Id
	@Column(nullable = false, unique = true)
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	@Column(nullable = false)
	private String firstName;
	private String lastName;

	@Column(nullable = false, unique = true)
	private String email;

	@Column(nullable = false, length = 60)
	private String password;

	private boolean enabled;
	private boolean tokenExpired;

	@ManyToMany(fetch = FetchType.EAGER)
	@JoinTable(name = "Registrations_Roles", joinColumns = @JoinColumn(name = "userId", referencedColumnName = "id"), inverseJoinColumns = @JoinColumn(name = "roleId", referencedColumnName = "id"))
	private Collection<RoleEntity> roles;

}